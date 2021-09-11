// +build linux freebsd

/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package zfs

import (
	"fmt"
	"context"
	"math"
	"path/filepath"
	"crypto/sha256"
	"bufio"
	"os/exec"
	"os"
	"bytes"
	"strings"
	"time"
	"strconv"
	"regexp"

	// "github.com/containerd/containerd/log"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	// "github.com/containerd/containerd/snapshots/storage"
	zfs "github.com/mistifyio/go-zfs"
	"github.com/pkg/errors"
)

const (
	// snapshotSuffix is used as follows:
	//	active := filepath.Join(dataset.Name, id)
	//      committed := active + "@" + snapshotSuffix
	snapshotSuffix = "snapshot"

	// Using this typed MaxInt64 to prevent integer overlow on 32bit
	maxSnapshotSize int64 = math.MaxInt64

	propKind = "containerd:kind"
	propParentName = "containerd:parent"
	propCreated = "containerd:created"
	propUpdated = "containerd:updated"
)

var zfsPropRegex = regexp.MustCompile("([^\\s]+)\\s+([^\\s]+)\\s+(.+)")

type snapshotter struct {
	dataset *zfs.Dataset
}

// NewSnapshotter returns a Snapshotter using zfs. Uses the provided
// root directory for snapshots and stores the metadata in
// a file in the provided root.
// root needs to be a mount point of zfs.
func NewSnapshotter(root string) (snapshots.Snapshotter, error) {
	m, err := mount.Lookup(root)
	if err != nil {
		return nil, err
	}
	if m.FSType != "zfs" {
		return nil, errors.Errorf("path %s must be a zfs filesystem to be used with the zfs snapshotter", root)
	}
	dataset, err := zfs.GetDataset(m.Source)
	if err != nil {
		return nil, err
	}

	b := &snapshotter{
		dataset: dataset,
	}
	return b, nil
}

var (
	zfsCreateProperties = map[string]string{
		"mountpoint": "legacy",
	}
)

func defaultNewProps(kind snapshots.Kind) (map[string]string, error) {
	props := zfsCreateProperties
	props[propKind] = fmt.Sprintf("%d", kind)
	props[propParentName] = "-"
	ts, err := time.Now().MarshalText()
	if err != nil {
		return props, err
	}
	props[propCreated] = string(ts)
	props[propUpdated] = string(ts)
	return props, nil
}

// createFilesystem creates but not mount.
func createFilesystem(datasetName string, kind snapshots.Kind) (*zfs.Dataset, error) {
	props, err := defaultNewProps(kind)
	if err != nil {
		return nil, err
	}
	return zfs.CreateFilesystem(datasetName, props)
}

// cloneFilesystem clones but not mount.
func cloneFilesystem(datasetName string, kind snapshots.Kind, snapshot *zfs.Dataset) (*zfs.Dataset, error) {
	props, err := defaultNewProps(kind)
	if err != nil {
		return nil, err
	}
	props[propParentName] = filepath.Base(snapshot.Name)
	return snapshot.Clone(datasetName, props)
}

// cloneFilesystem clones but not mount.
func cloneFilesystem_(datasetName string, snapshot *zfs.Dataset, propOverrides map[string]string) (*zfs.Dataset, error) {
	props := zfsCreateProperties
	for k, v := range propOverrides {
		props[k] = v
	}
	return snapshot.Clone(datasetName, props)
}

func destroy(dataset *zfs.Dataset) error {
	return dataset.Destroy(zfs.DestroyDefault)
}

func destroySnapshot(dataset *zfs.Dataset) error {
	return dataset.Destroy(zfs.DestroyDeferDeletion)
}

// Stat returns the info for an active or committed snapshot by name or
// key.
//
// Should be used for parent resolution, existence checks and to discern
// the kind of snapshot.
func (z *snapshotter) Stat(ctx context.Context, key string) (snapshots.Info, error) {

	props, err := z.getProps(key)
	if err != nil {
		return snapshots.Info{}, err
	}

	return z.infoFromProps(filepath.Base(z.getDatasetName(key)), props)
}

// Usage retrieves the disk usage of the top-level snapshot.
func (z *snapshotter) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	return z.usage(ctx, key)
}

func (z *snapshotter) usage(ctx context.Context, key string) (snapshots.Usage, error) {
	activeName := z.getDatasetName_(key)
	sDataset, err := zfs.GetDataset(activeName)

	if err != nil {
		return snapshots.Usage{}, err
	}

	if int64(sDataset.Used) > maxSnapshotSize {
		return snapshots.Usage{}, errors.Errorf("Dataset size exceeds maximum snapshot size of %d bytes", maxSnapshotSize)
	}

	usage := snapshots.Usage{
		Size:   int64(sDataset.Used),
		Inodes: -1,
	}

	return usage, nil
}

// Walk the committed snapshots.
func (z *snapshotter) Walk(ctx context.Context, fn snapshots.WalkFunc, filters ...string) error {
	// Granted this is gonna be slooow.. Still a test implementation, this, though

	//type WalkFunc func(context.Context, Info) error
	//all, err := z.getPropsForMultipleDatasets("")
	all := propCache

	for name, v := range all {
		info, err := z.infoFromProps(filepath.Base(name), v)
		if err != nil {
			return err
		}
		err = fn(ctx, info)
		if err != nil {
			return err
		}
	}

	return nil
}

func (z *snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return z.createSnapshot(ctx, snapshots.KindActive, key, parent, opts...)
}

func (z *snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return z.createSnapshot(ctx, snapshots.KindView, key, parent, opts...)
}

func getSnapshotID(key string) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%s", key)))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (z *snapshotter) getDatasetName(snapshotID string) string {
	return filepath.Join(z.dataset.Name, snapshotID)
} 

func (z *snapshotter) getDatasetName_(key string) string {
	return z.getDatasetName(getSnapshotID(key))
}

func (z *snapshotter) infoFromProps(name string, props map[string]string) (snapshots.Info, error) {
	var labels = make(map[string]string)
	fmt.Printf("infoFromProps: props: %v, kind: %d\n", props, props[propKind])

	kind, perr := strconv.ParseInt(props[propKind], 10, 0)
	if perr != nil {
		return snapshots.Info{}, perr
	}

	var created time.Time
	cerr := created.UnmarshalText([]byte(props[propCreated]))
	if cerr != nil {
		return snapshots.Info{}, cerr
	}
	var updated time.Time
	uerr := updated.UnmarshalText([]byte(props[propUpdated]))
	if uerr != nil {
		return snapshots.Info{}, uerr
	}

	return snapshots.Info{
		Kind:	snapshots.Kind(kind),
		Name: name,
		Parent: props[propParentName],
		Labels: labels,
		Created: created,
		Updated: updated,
	}, nil
}

var propCache = make(map[string]map[string]string)

func (z *snapshotter) getProps(key string) (map[string]string, error) {
	dummy := make(map[string]string)
	if propCache[key] != nil {
		return propCache[key], nil
	}
	res, err := z.getPropsForMultipleDatasets(key)
	if err != nil {
		return dummy, err
	}
	for _, v := range res {
		propCache[key] = v
		return v, nil
	}
	
	// should never happen (TODO: maybe return an error here actually)
	return dummy, nil
}

func (z *snapshotter) getPropsForMultipleDatasets(key string) (map[string]map[string]string, error) {

	var props = make(map[string]map[string]string)

	propNames := []string{propKind, propParentName, propCreated, propUpdated}
	propNamesStr := strings.Join(propNames, ",")

	var zfsPath string
	recursive := false
	if key != "" {
		zfsPath = z.getDatasetName_(key)
	} else {
		recursive = true
		zfsPath = z.dataset.Name
	}

	var args []string
	if recursive {
		args = []string{
			"get",
			"-o", "name,property,value",
			"-H",
			"-s", "local",
			"-r",
			propNamesStr,
			zfsPath,
		}
	} else {
		args = []string{
			"get",
			"-o", "name,property,value",
			"-H",
			"-s", "local",
			propNamesStr,
			zfsPath,
		}
	}

	cmd := exec.Command("zfs", args...)

	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		return props, err
	}
	scanner := bufio.NewScanner(bytes.NewReader(stdout.Bytes()))
	for scanner.Scan() {
		parts := zfsPropRegex.FindStringSubmatch(scanner.Text())
		if len(parts) == 4 {
			if props[parts[1]] == nil {
				props[parts[1]] = make(map[string]string)
			}
			props[parts[1]][parts[2]] = strings.TrimSpace(parts[3])
		}
	}

	return props, nil
}

func (z *snapshotter) createSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {

	var parentSnapshotID string
	var parentDataset *zfs.Dataset
	var perr error
	if parent != "" || parent == "-" {
		parentSnapshotID = getSnapshotID(parent)
		parentDatasetName := z.getDatasetName(parentSnapshotID)
		parentDataset, perr = zfs.GetDataset(parentDatasetName)
		if perr != nil {
			return nil, perr
		}
	}
	
	targetName := z.getDatasetName_(key)
	var target *zfs.Dataset
	var err error

	if parentDataset == nil {
		target, err = createFilesystem(targetName, kind)
		if err != nil {
			return nil, err
		}
	} else {
		parent0Name := filepath.Join(z.dataset.Name, parentSnapshotID) + "@" + snapshotSuffix
		parent0, err := zfs.GetDataset(parent0Name)
		if err != nil {
			return nil, err
		}
		target, err = cloneFilesystem(targetName, kind, parent0)
		if err != nil {
			return nil, err
		}
	}

	readonly := kind == snapshots.KindView
	return z.mounts(target, readonly)
}

func (z *snapshotter) mounts(dataset *zfs.Dataset, readonly bool) ([]mount.Mount, error) {
	var options []string
	if readonly {
		options = append(options, "ro")
	}
	return []mount.Mount{
		{
			Type:    "zfs",
			Source:  dataset.Name,
			Options: options,
		},
	}, nil
}

func (z *snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) (err error) {
	
	activeName := z.getDatasetName_(key)

	before := time.Now()

	_, err = zfs.GetDataset(activeName)
	if err != nil {
		return err
	}

	normalizedName := z.getDatasetName_(name)

	args := []string{
		"rename",
		activeName,
		normalizedName,
	}

	cmd := exec.Command("zfs", args...)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	if err != nil {
		return err
	}

	renamed, rnerr := zfs.GetDataset(normalizedName)
	if rnerr != nil {
		return rnerr
	}

	_, serr := renamed.Snapshot(snapshotSuffix, false)
	if serr != nil {
		return serr
	}
	after := time.Now()
	fmt.Println("snapshotDataset: ", after.Sub(before))

	return nil
}

// Mounts returns the mounts for the transaction identified by key. Can be
// called on an read-write or readonly transaction.
//
// This can be used to recover mounts after calling View or Prepare.
func (z *snapshotter) Mounts(ctx context.Context, key string) ([]mount.Mount, error) {
	sName := z.getDatasetName_(key)
	sDataset, err := zfs.GetDataset(sName)
	if err != nil {
		return nil, err
	}
	return z.mounts(sDataset, false)
}

// Remove abandons the transaction identified by key. All resources
// associated with the key will be removed.
func (z *snapshotter) Remove(ctx context.Context, key string) (err error) {
	info, err := z.Stat(ctx, key)
	if err != nil {
		return err
	}
	datasetName := z.getDatasetName(info.Name)
	if info.Kind == snapshots.KindCommitted {
		snapshotName := datasetName + "@" + snapshotSuffix
		snapshot, err := zfs.GetDataset(snapshotName)
		if err != nil {
			return err
		}
		if err = destroySnapshot(snapshot); err != nil {
			return err
		}
	}
	dataset, err := zfs.GetDataset(datasetName)
	if err != nil {
		return err
	}
	if err = destroy(dataset); err != nil {
		return err
	}
	return err
}

func (o *snapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error) {
	//TODO: noop until we support labeling
	return info, nil
}

func (o *snapshotter) Close() error {
	return nil
}
