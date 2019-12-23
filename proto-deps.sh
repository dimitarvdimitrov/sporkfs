#!/bin/bash

mkdir -p third_party/etcd/raftpb
mkdir -p third_party/gogoproto

tmp_dir=$(mktemp -d)

git clone --quiet --depth 1 https://github.com/gogo/protobuf.git $tmp_dir
cp $tmp_dir/gogoproto/*.proto third_party/gogoproto/
cp `go env GOPATH`/pkg/mod/github.com/coreos/etcd*/raft/raftpb/*.proto third_party/etcd/raftpb/

chmod -R u+rw third_party/

rm -rf $tmp_dir
