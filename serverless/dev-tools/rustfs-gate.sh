#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
set -euo pipefail
cd "$(dirname "$0")/../.."

mode=${1:-smoke}
if [[ $mode != smoke && $mode != fleet ]]; then
  echo "usage: bash serverless/dev-tools/rustfs-gate.sh [smoke|fleet] [Gradle options...]" >&2
  exit 2
fi
if (( $# > 0 )); then shift; fi
if [[ -r /proc/self/cgroup && " $* " != *" -Dopensearch.cgroups.hierarchy.override="* ]]; then
  cgroup=$(awk -F: '$1 == 0 && $2 == "" { print $3; exit }' /proc/self/cgroup)
  if [[ -n $cgroup && $cgroup != / && -r /sys/fs/cgroup$cgroup/memory.max ]]; then
    set -- "$@" "-Dopensearch.cgroups.hierarchy.override=$cgroup"
  fi
fi

work=$(mktemp -d)
rustfs_pid=
cleanup() {
  if [[ -n $rustfs_pid ]]; then
    kill "$rustfs_pid" 2>/dev/null || true
    wait "$rustfs_pid" 2>/dev/null || true
  fi
  rm -rf -- "$work"
}
trap cleanup EXIT

version=1.0.5
binary=${RUSTFS_BINARY:-$work/rustfs}
if [[ ! -x $binary && -z ${RUSTFS_BINARY:-} ]]; then
  asset=rustfs-linux-x86_64-gnu-v$version.zip
  curl --fail --silent --show-error --location --retry 3 \
    "https://github.com/rustfs/rustfs/releases/download/$version/$asset" -o "$work/$asset"
  (cd "$work"; echo "5addae2f74756bf80104d0975e6347cda843bc2daffdfcd1f0cb80118b95cbd0  $asset" | sha256sum -c)
  unzip -q "$work/$asset" -d "$work"
fi
"$binary" --version | grep -Fx "rustfs $version"

port=${RUSTFS_PORT:-19200}
endpoint=http://127.0.0.1:$port
if curl --noproxy '*' --silent --connect-timeout 1 --max-time 2 "$endpoint" >/dev/null; then
  echo "RustFS test port $port is already in use" >&2
  exit 1
fi
mkdir -p "$work/data"
RUSTFS_ACCESS_KEY=rustfsadmin RUSTFS_SECRET_KEY=rustfsadmin RUSTFS_CONSOLE_ENABLE=false \
  RUSTFS_ADDRESS="127.0.0.1:$port" "$binary" "$work/data" >"$work/rustfs.log" 2>&1 &
rustfs_pid=$!
ready=false
for ((i=0; i<60; i++)); do
  if ! kill -0 "$rustfs_pid" 2>/dev/null; then break; fi
  if curl --noproxy '*' --fail --silent --max-time 2 "$endpoint/health" >/dev/null; then
    ready=true
    break
  fi
  sleep 1
done
if [[ $ready != true ]]; then
  cat "$work/rustfs.log" >&2
  exit 1
fi

if [[ $mode == smoke ]]; then
  bash gradlew "$@" \
    :serverless:shell:compileJava :serverless:testkit:compileGcsEmulatorTestJava \
    :serverless:testkit:test --tests '*RequestBudget*' --tests '*IdleIndexRequestTests' --tests '*NodeLogTakeoverTests' \
    --tests '*ServerlessAuthorizationTests.testAFilterRefusesAWriteForOneCallerAndNotAnother' \
    --tests '*ServerlessSearchPathTests.testWriterActivationLeavesAPinnedReaderIntact' \
    --tests '*ServerlessLogMessageTests' \
    :serverless:testkit:s3Test --tests '*RustfsBlobContainerConformanceTests.testPutIfAbsentBlobWritesAdmitExactlyOne' \
    --tests '*RustfsBlobContainerConformanceTests.testCompareAndSwapAdmitsExactlyOneWinner' \
    --tests '*ServerlessOnObjectStoreTests.testASuccessorRecoversFromTheBucketAlone' \
    --tests '*RustfsNodeLogTakeoverTests.testTakeoverRefusesMissingAcknowledgedHistory' \
    --tests '*RustfsNodeLogTakeoverTests.testTakeoverDistinguishesTheTwoOutcomesOfAnAmbiguousAppend' \
    --tests '*RustfsNodeLogTakeoverIntegrationTests.testACrashedWritersUnpublishedWritesSurviveTwoTakeovers' \
    :serverless:testkit:processTest --tests '*ServerlessBucketContentionTests.testFullFleetHardStopRecoversUnpublishedWritesFromTheSameBucket' \
    --tests '*ServerlessFleetTests.testRestErrorsDoNotLogQueryParameters' \
    -Dtests.serverless.rustfs.endpoint="$endpoint" -Dtests.serverless.s3.endpoint="$endpoint" \
    -Dtests.serverless.s3.access_key=rustfsadmin -Dtests.serverless.s3.secret_key=rustfsadmin -Dtests.serverless.rustfs.gate=true

else
  bash gradlew "$@" :serverless:testkit:fleetTest \
    -Dtests.fleet.endpoint="$endpoint" -Dtests.fleet.access_key=rustfsadmin -Dtests.fleet.secret_key=rustfsadmin \
    -Dtests.fleet.kill_isolation=true
fi

python3 - "$mode" <<'PY'
from pathlib import Path
from sys import argv
from xml.etree import ElementTree

smoke = {
    "testPutIfAbsentBlobWritesAdmitExactlyOne",
    "testCompareAndSwapAdmitsExactlyOneWinner",
    "testASuccessorRecoversFromTheBucketAlone",
    "testTakeoverRefusesMissingAcknowledgedHistory",
    "testTakeoverDistinguishesTheTwoOutcomesOfAnAmbiguousAppend",
    "testACrashedWritersUnpublishedWritesSurviveTwoTakeovers",
    "testFullFleetHardStopRecoversUnpublishedWritesFromTheSameBucket",
    "testRestErrorsDoNotLogQueryParameters",
}
if argv[1] == "smoke":
    required = smoke
    tasks = ("s3Test", "processTest")
else:
    required = {"testAFleetUnderLoadAndFailureLosesNoAcknowledgedWrite"}
    tasks = ("fleetTest",)

passed = set()
results = Path("serverless/testkit/build/test-results")
for task in tasks:
    for report in (results / task).glob("TEST-*.xml"):
        for case in ElementTree.parse(report).iter("testcase"):
            if all(case.find(status) is None for status in ("failure", "error", "skipped")):
                passed.add(case.get("name"))
if missing := required - passed:
    raise SystemExit("Required RustFS tests did not pass: " + ", ".join(sorted(missing)))
PY
