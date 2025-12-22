# Release Process

jetcd uses the [Axion Release Plugin](https://github.com/allegro/axion-release-plugin) for versioning.

## Check Current Version

```bash
./gradlew currentVersion
```

## Create a Release

```bash
# 1. Create release (tags and updates version)
./gradlew release

# 2. Verify tag was created
git tag | grep jetcd

# 3. Push tag to trigger CI publish
git push --tags

# 4. Publish to Maven Central (if not automated)
./gradlew publish
```

## Set Next Development Version

```bash
./gradlew markNextVersion -Prelease.version=X.Y.Z
```

## Dry Run

To test without making changes:

```bash
./gradlew release -Prelease.dryRun
```

