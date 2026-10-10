#!/usr/bin/env bash
# Keep the fork's main identical to upstream/main. Fork-only work lives on other branches.
#
#   scripts/sync-fork.sh            fetch upstream, fast-forward main, push origin main
#   scripts/sync-fork.sh --no-push  do everything except the push
#
# main only ever fast-forwards. If it cannot (local commits on main, or origin/main has
# diverged), the script stops and changes nothing; it never force-pushes.
#
# When main is the checked-out branch here, the fast-forward also updates the working tree. If
# main is checked out in another worktree (for example the checkout a live daemon runs from), run
# the script from a different branch: it then leaves local main and every file alone and pushes
# upstream/main straight to origin/main, so nothing under the daemon changes and no rebuild is
# needed. Run `scripts/sync-fork.sh` from that checkout later to move its main.
set -euo pipefail

push=1
[ "${1:-}" = "--no-push" ] && push=0

git fetch upstream
git fetch origin

if ! git merge-base --is-ancestor main upstream/main; then
  echo "main has commits that upstream/main lacks; move them to a branch first:" >&2
  git log --oneline upstream/main..main >&2
  exit 1
fi

current="$(git branch --show-current)"
main_elsewhere=0
if [ "$current" != "main" ] && git worktree list --porcelain | grep -qx "branch refs/heads/main"; then
  main_elsewhere=1
fi

if [ "$current" = "main" ]; then
  git merge --ff-only upstream/main
elif [ "$main_elsewhere" -eq 0 ]; then
  git fetch upstream main:main
else
  echo "main is checked out in another worktree; leaving local main and its files alone."
fi

if [ "$push" -eq 1 ]; then
  if [ "$main_elsewhere" -eq 1 ]; then
    git push origin upstream/main:refs/heads/main
  else
    git push origin main
  fi
fi

echo "main: $(git rev-parse --short main)  upstream/main: $(git rev-parse --short upstream/main)  origin/main: $(git rev-parse --short origin/main)"
