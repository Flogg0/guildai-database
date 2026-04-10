# Copyright 2017-2023 Posit Software, PBC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import configparser
import os
import platform
import re
import subprocess
from email.parser import Parser

from setuptools import find_packages, setup
from setuptools.command.build_py import build_py

GUILD_DIST_BASENAME = "guildai.dist-info"


def _read_version():
    # Avoid `import guild` — the source tree isn't on sys.path during
    # PEP 517 isolated builds, and importing guild pulls in runtime
    # deps that shouldn't be required at build time.
    with open(os.path.join("guild", "__init__.py"), encoding="utf-8") as f:
        src = f.read()
    m = re.search(r'^__version__\s*=\s*["\']([^"\']+)["\']', src, re.MULTILINE)
    if not m:
        raise RuntimeError("could not find __version__ in guild/__init__.py")
    return m.group(1)

if platform.system() == "Windows":
    NPM_CMD = "npm.cmd"
else:
    NPM_CMD = "npm"


def guild_dist_info():
    metadata_path = os.path.join(GUILD_DIST_BASENAME, "METADATA")
    with open(metadata_path, encoding="utf-8") as f:
        pkg_info = Parser().parse(f)
    assert pkg_info.get("Name") == "guildai", pkg_info.get("Name")
    entry_points_path = os.path.join(GUILD_DIST_BASENAME, "entry_points.txt")
    parser = configparser.ConfigParser()
    parser.optionxform = str  # preserve case
    parser.read(entry_points_path, encoding="utf-8")
    entry_points = {
        group: [f"{name} = {value}" for name, value in parser.items(group)]
        for group in parser.sections()
    }
    return pkg_info, entry_points


def guild_packages():
    return find_packages(exclude=["guild.tests", "guild.tests.*"])


PKG_INFO, ENTRY_POINTS = guild_dist_info()


class Build(build_py):
    """Extension of default build with additional pre-processing.

    In preparation for setuptool's default build, we perform these
    additional pre-processing steps:

    - Build view distribution

    See MANIFEST.in for a complete list of data files includes in the
    Guild distribution.
    """
    def run(self):
        if os.getenv("BUILD_GUILD_VIEW") == "1":
            _check_npm()
            _build_view_dist()
        build_py.run(self)


def _check_npm():
    try:
        subprocess.check_output([NPM_CMD, "--version"])
    except OSError as e:
        raise SystemExit(f"error checking npm: {e}") from e


def _build_view_dist():
    """Build view distribution."""
    subprocess.check_call([NPM_CMD, "install"], cwd="./guild/view")
    subprocess.check_call([NPM_CMD, "run", "build"], cwd="./guild/view")


setup(
    # Setup class config
    cmdclass={"build_py": Build},
    # Attributes from dist-info
    name="guildai",
    version=_read_version(),
    description=PKG_INFO.get("Summary"),
    install_requires=PKG_INFO.get_all("Requires-Dist"),
    long_description=PKG_INFO.get_payload(),
    long_description_content_type="text/markdown",
    url=PKG_INFO.get("Home-page"),
    maintainer=PKG_INFO.get("Author"),
    maintainer_email=PKG_INFO.get("Author-email"),
    entry_points=ENTRY_POINTS,
    classifiers=PKG_INFO.get_all("Classifier"),
    license=PKG_INFO.get("License"),
    keywords=PKG_INFO.get("Keywords"),
    # Package data
    packages=guild_packages(),
    include_package_data=True,
    # Other package info
    zip_safe=False,
    scripts=["./guild/scripts/guild-env"],
)
