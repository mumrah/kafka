Mapping of GitHub usernames to full name and email. This data is used to automatically
generate "Reviewers" Git trailers for our commit messages

To add someone, use `git config` in this branch and commit the changes

```
git config --file committers.gitconfig committer.anexample.name Alice N Example
git config --file committers.gitconfig committer.anexample.email alice@example.org
```
