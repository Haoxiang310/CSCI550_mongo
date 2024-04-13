DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

set -euo pipefail

cd src
commit_date=$(date -d "$(git log -1 -s --format=%ci)" "+%s")
last_run_date=$(cat ../antithesis_last_push.txt || echo 0)
if [ "${is_patch}" != "true" ] && [ "${last_run_date}" -gt "${commit_date}" ]; then
  echo -e "Refusing to push new antithesis images because this commit is older\nthan the last pushed commit"
  exit 0
fi
cd ..

# check that the binaries in dist-test are linked to libvoidstar
ldd src/dist-test/bin/mongod | grep libvoidstar
ldd src/dist-test/bin/mongos | grep libvoidstar
ldd src/dist-test/bin/mongo | grep libvoidstar

# prepare the image building environment
cp -rf src/buildscripts/antithesis/ antithesis
# due to gitignore, we can't commit a folder called logs, so make it here
mkdir -p antithesis/topologies/replica_set/{logs,data}/database{1,2,3}
mkdir -p antithesis/topologies/replica_set/{logs,data}/workload
mkdir -p antithesis/topologies/sharded_cluster/{logs,data}/database{1,2,3,4,5,6}
mkdir -p antithesis/topologies/sharded_cluster/{logs,data}/configsvr{1,2,3}
mkdir -p antithesis/topologies/sharded_cluster/{logs,data}/{mongos,workload}

# extract debug symbols into usr/bin and have directory structure mimic Docker container
mkdir -p antithesis/topologies/sharded_cluster/debug/usr/bin
tar -zxvf src/mongo-debugsymbols.tgz -C antithesis/topologies/sharded_cluster/debug
cp antithesis/topologies/sharded_cluster/debug/dist-test/bin/* antithesis/topologies/sharded_cluster/debug/usr/bin
rm -rf antithesis/topologies/sharded_cluster/debug/dist-test

# recompress debug symbols
tar -czvf antithesis/topologies/sharded_cluster/debug/mongo-debugsymbols.tgz -C antithesis/topologies/sharded_cluster/debug/ usr
rm -rf antithesis/topologies/sharded_cluster/debug/usr

echo "${revision}" > antithesis/topologies/sharded_cluster/data/workload/mongo_version.txt

# copy ... to the build context
# resmoke
cp -rf src antithesis/base_images/workload/src
# mongo binary
cp src/dist-test/bin/mongo antithesis/base_images/workload
# libvoidstar
cp /usr/lib/libvoidstar.so antithesis/base_images/workload
# these aren't needed for the workload image, so get rid of them
rm -rf antithesis/base_images/workload/src/dist-test
# all mongodb binaries
cp -rf src/dist-test antithesis/base_images/mongo_binaries
cp /usr/lib/libvoidstar.so antithesis/base_images/mongo_binaries/

# push images as evergreen-latest-${branch_name}, unless it's a patch
tag="evergreen-latest-${branch_name}"
if [ "${is_patch}" = "true" ]; then
  tag="evergreen-patch"
fi

if [ -n "${antithesis_image_tag:-}" ]; then
  echo "Using provided tag: '$antithesis_image_tag' for docker pushes"
  tag=$antithesis_image_tag
fi

cd antithesis/base_images/mongo_binaries
sudo docker build . -t mongo-binaries:$tag

cd ../workload
sudo docker build . -t workload:$tag

cd ../../topologies/replica_set
sed -i s/evergreen-latest-master/$tag/ docker-compose.yml
sudo docker build . -t repl-set-config:$tag

cd ../sharded_cluster
sed -i s/evergreen-latest-master/$tag/ docker-compose.yml
sudo docker build . -t sharded-cluster-config:$tag

# login, push, and logout
echo "${antithesis_repo_key}" > mongodb.key.json
cat mongodb.key.json | sudo docker login -u _json_key https://us-central1-docker.pkg.dev --password-stdin
rm mongodb.key.json

# tag and push to the registry
sudo docker tag "mongo-binaries:$tag" "us-central1-docker.pkg.dev/molten-verve-216720/mongodb-repository/mongo-binaries:$tag"
sudo docker push "us-central1-docker.pkg.dev/molten-verve-216720/mongodb-repository/mongo-binaries:$tag"

sudo docker tag "workload:$tag" "us-central1-docker.pkg.dev/molten-verve-216720/mongodb-repository/workload:$tag"
sudo docker push "us-central1-docker.pkg.dev/molten-verve-216720/mongodb-repository/workload:$tag"

sudo docker tag "repl-set-config:$tag" "us-central1-docker.pkg.dev/molten-verve-216720/mongodb-repository/repl-set-config:$tag"
sudo docker push "us-central1-docker.pkg.dev/molten-verve-216720/mongodb-repository/repl-set-config:$tag"

sudo docker tag "sharded-cluster-config:$tag" "us-central1-docker.pkg.dev/molten-verve-216720/mongodb-repository/sharded-cluster-config:$tag"
sudo docker push "us-central1-docker.pkg.dev/molten-verve-216720/mongodb-repository/sharded-cluster-config:$tag"

sudo docker logout https://us-central1-docker.pkg.dev

if [ "${is_patch}" != "true" ]; then
  echo "$commit_date" > antithesis_next_push.txt
fi
