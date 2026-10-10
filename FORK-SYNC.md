# Keeping this fork in sync with upstream

`origin` is the fork (`phamhung075/openrig`). `upstream` is `mvschwarz/openrig`.

- `main` is a mirror of `upstream/main`. Do not commit to it.
- Fork-only work lives on other branches, based on `upstream/main`. The current one is
  `mine/agy-on-0.6.9` (agy runtime, Enter-consumed check after `rig send`).

## One-time setup

```bash
git remote add upstream https://github.com/mvschwarz/openrig   # if missing
git branch --set-upstream-to=upstream/main main                # main follows upstream
git config branch.main.pushRemote origin                       # ...and pushes to the fork
git config remote.pushDefault origin
```

## Sync

```bash
scripts/sync-fork.sh            # fetch upstream, fast-forward main, push origin main
scripts/sync-fork.sh --no-push  # same, without the push
```

The same steps by hand:

```bash
git fetch upstream && git fetch origin
git switch main && git merge --ff-only upstream/main
git push origin main
```

`main` only fast-forwards. If it cannot, the script stops without changing anything and does
not force-push.

## The daemon's checkout

If `main` is checked out in the checkout that runs a live daemon, running the script there
updates that checkout's files, so rebuild afterwards (`npm install && npm run build`).

To sync without touching it, run the script from a different branch or worktree. When `main` is
checked out elsewhere, the script leaves local `main` and every file alone and pushes
`upstream/main` straight to `origin/main`. Move the daemon checkout's `main` later, when a
rebuild is acceptable, by running the script there.

## Updating a fork-only branch

```bash
git switch mine/agy-on-0.6.9
git rebase main     # or: git merge main
npm run build && npm test
git push --force-with-lease origin mine/agy-on-0.6.9   # only after a rebase
```
