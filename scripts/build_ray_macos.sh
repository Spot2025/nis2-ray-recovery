#!/bin/bash
# Build the assigned Ray revision natively, without changing the project's .venv.
set -euo pipefail

readonly RAY_REVISION=2ff4d94078ee94b3c9e6e4552ad84f78bad7f75f
readonly BAZEL_VERSION=7.5.0
readonly BAZEL_SHA256=eb2f29a57d7ac28645787b8bab8c5e11e6d1d0432c34084fc22acfdacf8f02dd

fail() { printf '%s\n' "ERROR: $*" >&2; exit 1; }

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
cd "$project_dir"
[[ $(uname -s) == Darwin && $(uname -m) == arm64 ]] || \
  fail 'This script requires native macOS ARM64.'
python_bin=$(command -v "${PYTHON_BIN:-python3.12}") || \
  fail 'Install native Python 3.12, or set PYTHON_BIN to its executable.'
"$python_bin" -c 'import platform, sys; assert sys.version_info[:2] == (3, 12); assert platform.machine() == "arm64"'
/usr/bin/xcode-select -p >/dev/null || fail 'Install Xcode Command Line Tools: xcode-select --install'
for command_name in git curl shasum; do
  command -v "$command_name" >/dev/null || fail "Missing command: $command_name"
done
[[ -x /usr/bin/clang && -x /usr/bin/clang++ ]] || fail 'Apple clang is required.'

# Bazel 7.5.0 failed to start from the Cyrillic project directory in our run.
# Keep both the source and Bazel output in a physical ASCII path.
build_root=${RAY_BUILD_ROOT:-$(mktemp -d /private/tmp/nis2-ray-build.XXXXXX)}
mkdir -p "$build_root"
build_root=$(CDPATH= cd -- "$build_root" && pwd -P)
"$python_bin" - "$build_root" <<'PY'
import sys
path = sys.argv[1]
assert path.isascii() and not any(c.isspace() for c in path), (
    'RAY_BUILD_ROOT must resolve to an ASCII path without whitespace', path
)
PY

venv_dir="$project_dir/work/venvs/ray-pinned"
tool_dir="$project_dir/work/tools/bin"
wheel_dir="$project_dir/work/build/wheels"
log_dir="$project_dir/work/setup/build-logs"
source_dir="$build_root/ray-src"
mkdir -p "$tool_dir" "$wheel_dir" "$log_dir" "$build_root/bin"
if [[ ! -e "$venv_dir" ]]; then
  "$python_bin" -m venv "$venv_dir"
fi
[[ -x "$venv_dir/bin/python" ]] || fail "Invalid virtual environment: $venv_dir"
"$venv_dir/bin/python" -c 'import platform, sys; assert sys.version_info[:2] == (3, 12); assert platform.machine() == "arm64"'

# Invoke Python through an ASCII path while building. The installed environment
# itself stays in work/, and the resulting wheel does not depend on /private/tmp.
if [[ ! -e "$build_root/venv" && ! -L "$build_root/venv" ]]; then
  ln -s "$venv_dir" "$build_root/venv"
fi
"$python_bin" - "$build_root/venv" "$venv_dir" <<'PY'
from pathlib import Path
import sys
assert Path(sys.argv[1]).resolve() == Path(sys.argv[2]).resolve(), 'Unexpected build venv'
PY
build_python="$build_root/venv/bin/python"
"$build_python" -m pip install \
  pip==26.2.2 setuptools==84.2.1 wheel==0.48.0 cython==3.3.0 numpy==2.5.4

bazel_binary="$tool_dir/bazel-$BAZEL_VERSION"
if [[ ! -f "$bazel_binary" ]]; then
  curl --fail --location --retry 3 --connect-timeout 15 --max-time 300 \
    "https://github.com/bazelbuild/bazel/releases/download/$BAZEL_VERSION/bazel-$BAZEL_VERSION-darwin-arm64" \
    -o "$bazel_binary.download"
  mv "$bazel_binary.download" "$bazel_binary"
fi
actual_bazel_sha=$(shasum -a 256 "$bazel_binary" | awk '{print $1}')
[[ "$actual_bazel_sha" == "$BAZEL_SHA256" ]] || fail 'Bazel SHA-256 mismatch.'
chmod +x "$bazel_binary"
"$bazel_binary" --version

if [[ ! -e "$source_dir" ]]; then
  git clone --filter=blob:none --no-checkout https://github.com/ray-project/ray.git "$source_dir"
  git -C "$source_dir" checkout --detach "$RAY_REVISION"
fi
# Reject another revision or source patches, including on a repeated invocation.
# Filling this upstream packaging placeholder is the only permitted difference.
"$build_python" - "$source_dir" "$RAY_REVISION" <<'PY'
from pathlib import Path
import subprocess
import sys

root, expected = Path(sys.argv[1]), sys.argv[2]
def git(*args):
    return subprocess.check_output(['git', *args], cwd=root, text=True).strip()

assert git('rev-parse', 'HEAD') == expected, 'Unexpected Ray source revision'
assert not git('diff', '--cached', '--name-only'), 'Unexpected staged source changes'
assert not git('ls-files', '--others', '--exclude-standard'), 'Unexpected untracked source files'
changed = git('diff', '--name-only').splitlines()
assert changed in ([], ['python/ray/_version.py']), ('Unexpected source changes', changed)
original = subprocess.check_output(
    ['git', 'show', 'HEAD:python/ray/_version.py'], cwd=root, text=True
)
assert '{{RAY_COMMIT_SHA}}' in original
stamped = original.replace('{{RAY_COMMIT_SHA}}', expected)
version_file = root / 'python/ray/_version.py'
assert version_file.read_text() in (original, stamped), 'Unexpected version metadata'
version_file.write_text(stamped)
print('Verified Ray source:', expected)
PY

export RAY_BUILD_ROOT="$build_root"
export RAY_BAZEL_BINARY="$bazel_binary"
cat > "$build_root/bin/bazel" <<'SH'
#!/bin/sh
set -eu
exec "$RAY_BAZEL_BINARY" \
  --output_user_root="$RAY_BUILD_ROOT/bazel-user-root" \
  --output_base="$RAY_BUILD_ROOT/bazel-output" "$@"
SH
chmod +x "$build_root/bin/bazel"
export BAZEL_PATH="$build_root/bin/bazel"
export PATH="$build_root/venv/bin:$build_root/bin:/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin"
export CC=/usr/bin/clang CXX=/usr/bin/clang++
export LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 MACOSX_DEPLOYMENT_TARGET=12.0
export RAY_DISABLE_EXTRA_CPP=1 RAY_BUILD_REDIS=0
export GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1 GRPC_PYTHON_BUILD_SYSTEM_ZLIB=1

jobs=${RAY_BUILD_JOBS:-6}
memory_mb=${RAY_BUILD_MEMORY_MB:-$(( $(/usr/sbin/sysctl -n hw.memsize) / 1024 / 1024 * 2 / 3 ))}
[[ "$jobs" =~ ^[1-9][0-9]*$ && "$memory_mb" =~ ^[1-9][0-9]*$ ]] || \
  fail 'Build jobs and memory must be positive integers.'
export BAZEL_ARGS="--jobs=$jobs --local_resources=cpu=$jobs --local_resources=memory=$memory_mb --show_progress_rate_limit=30"
wheel_staging=$(mktemp -d "$build_root/wheels.XXXXXX")
printf 'Build tree: %s\nParallel jobs: %s; memory budget: %s MiB\n' "$build_root" "$jobs" "$memory_mb"
(
  cd "$source_dir/python"
  "$build_python" -m pip wheel --no-build-isolation --no-deps --verbose . \
    --wheel-dir "$wheel_staging"
) 2>&1 | tee "$log_dir/build-ray-macos.log"
(
  cd "$source_dir"
  "$BAZEL_PATH" shutdown
)

wheels=("$wheel_staging"/ray-*.whl)
[[ ${#wheels[@]} -eq 1 && -f "${wheels[0]}" ]] || fail 'Expected exactly one Ray wheel.'
wheel_path="$wheel_dir/$(basename -- "${wheels[0]}")"
cp "${wheels[0]}" "$wheel_path"
"$venv_dir/bin/python" -m pip install --force-reinstall --no-deps "$wheel_path"
# Install the core runtime dependencies declared by this wheel. This resolves
# the same local artifact; it does not select a different Ray from the index.
"$venv_dir/bin/python" -m pip install "$wheel_path"
"$venv_dir/bin/python" -m pip check
"$venv_dir/bin/python" - "$RAY_REVISION" "$wheel_path" "$project_dir" <<'PY'
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import sys
import ray

expected, wheel, project = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
assert ray.__commit__ == expected, ('Installed Ray revision mismatch', ray.__commit__)
record = {
    'checked_at_utc': datetime.now(timezone.utc).isoformat(),
    'python': platform.python_version(),
    'platform': platform.platform(),
    'ray': ray.__version__,
    'ray_commit': ray.__commit__,
    'wheel': str(wheel.relative_to(project)),
    'wheel_sha256': hashlib.sha256(wheel.read_bytes()).hexdigest(),
    'source_change': 'Only replace the upstream _version.py commit placeholder with the verified Git SHA.',
}
(project / 'work/setup/ray-pinned-macos-build.json').write_text(json.dumps(record, indent=2) + '\n')
print(json.dumps(record, indent=2))
PY
"$venv_dir/bin/python" -m pip freeze > "$project_dir/work/setup/ray-pinned-macos-freeze.txt"
printf '\nReady: %s\nWheel: %s\n' "$venv_dir/bin/python" "$wheel_path"
