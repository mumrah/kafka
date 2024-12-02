# ASF Committers File

This orphaned branch includes a file named `committers.gitconfig` which is used to define
a mapping of GitHub usernames to a committers preferred name and email address. This data 
is _not_ authoritative, but rather is used for some automations.

To modify this data set, checkout the `asf-committers` branch. Since this is an orphaned branch
use `git switch` to avoid leaving behind all the tracked files.

```
git fetch origin asf-committers
git switch asf-committers
```

Create a branch to make modifications

```
git checkout -b add-me-to-asf-committers
```

Use `git config` to add your preferred display name and email.

```
git config --file committers.gitconfig --add committer.anexample.name "Alice N Example"
git config --file committers.gitconfig --add committer.anexample.email alice@example.org
```

To update an existing entry, you can edit the committers.gitconfig file directly or use "--unset" 
to remove an existing value before updating it.

Push to your changes to a fork and open a PR with `asf-committers` as the base branch.

To use this data, the file can be used directly with `--file` or as a Git ref with `--blob`

```
git config --blob asf-committers:committers.gitconfig --get committer.anexample.name
> Alice N Example
```

