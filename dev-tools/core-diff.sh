#!/usr/bin/env bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

# Reports what this branch changes outside serverless/ compared with upstream OpenSearch, so the cost of the next
# rebase onto upstream main is visible, and optionally fails when that cost grows past a budget.
#
# Files the branch adds cannot conflict on a rebase; lines changed in files upstream also has can. The report keeps
# the two apart, and the budget counts only the second.

set -e -o pipefail
export LC_ALL=C

usage() {
  cat <<'EOF'
usage: dev-tools/core-diff.sh [--base <ref>] [--budget <lines>] [--exclude <path>]... [--quiet]

  --base <ref>       the upstream commit to compare against; defaults to the merge base of HEAD and upstream/main
  --budget <lines>   fail (exit 1) when the lines added plus removed in upstream files exceed this number
  --exclude <path>   leave a path out of the report, in addition to serverless/ and .github/; repeatable
  --quiet            print only the summary

To compare against upstream main, add the remote once:
  git remote add upstream https://github.com/opensearch-project/OpenSearch.git
  git fetch upstream main
EOF
}

base=""
budget=""
quiet=false
excludes=("serverless" ".github")

while [ $# -gt 0 ]; do
  case "$1" in
    --base) base="$2"; shift 2 ;;
    --budget) budget="$2"; shift 2 ;;
    --exclude) excludes+=("${2%/}"); shift 2 ;;
    --quiet) quiet=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [ -n "$budget" ] && ! [[ "$budget" =~ ^[0-9]+$ ]]; then
  echo "--budget takes a whole number of lines, got: $budget" >&2
  exit 2
fi

if [ -z "$base" ]; then
  if ! git rev-parse --verify --quiet upstream/main >/dev/null; then
    echo "no upstream/main ref; pass --base <ref> or add the upstream remote:" >&2
    echo "  git remote add upstream https://github.com/opensearch-project/OpenSearch.git && git fetch upstream main" >&2
    exit 2
  fi
  base=$(git merge-base HEAD upstream/main)
fi
git rev-parse --verify --quiet "$base^{commit}" >/dev/null || { echo "not a commit: $base" >&2; exit 2; }

pathspec=(.)
for path in "${excludes[@]}"; do
  pathspec+=(":(exclude)$path")
done

# One row per file: status, lines added, lines removed, path. Renames are reported as a delete plus an add, which is
# what a rebase sees. Binary files count as zero lines.
rows=$(
  join -t $'\t' -1 2 -2 3 \
    <(git diff --no-renames --name-status "$base" HEAD -- "${pathspec[@]}" | sort -t $'\t' -k 2,2) \
    <(git diff --no-renames --numstat "$base" HEAD -- "${pathspec[@]}" | sed 's/^-\t-\t/0\t0\t/' | sort -t $'\t' -k 3,3) \
    -o 1.1,2.1,2.2,0 || true
)

if [ "$quiet" = false ] && [ -n "$rows" ]; then
  printf '%-8s %7s %7s  %s\n' status added removed path
  printf '%s\n' "$rows" | sort -t $'\t' -k 1,1 -k 4,4 | while IFS=$'\t' read -r status added removed path; do
    case "$status" in
      A) kind=new ;;
      D) kind=deleted ;;
      *) kind=modified ;;
    esac
    printf '%-8s %7s %7s  %s\n' "$kind" "+$added" "-$removed" "$path"
  done
  echo
fi

summary=$(printf '%s\n' "$rows" | awk -F '\t' '
  NF == 0 { next }
  $1 == "A" { new++; newLines += $2; next }
  { changed++; added += $2; removed += $3 }
  END { printf "%d %d %d %d %d\n", changed, added, removed, new, newLines }
')
read -r changed added removed new newLines <<<"$summary"

echo "base: $(git rev-parse --short "$base")  excluded: ${excludes[*]}"
echo "upstream files changed: $changed (+$added -$removed)  <- what a rebase can conflict on"
echo "files added: $new (+$newLines)"

if [ -n "$budget" ]; then
  total=$((added + removed))
  if [ "$total" -gt "$budget" ]; then
    echo "over budget: $total changed lines in upstream files, budget $budget" >&2
    exit 1
  fi
  echo "within budget: $total of $budget changed lines in upstream files"
fi
