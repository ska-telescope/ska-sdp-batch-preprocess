###############################################################################
# Dockerfile with a portable build of DP3 (arch=x86_64_v2)
###############################################################################
#
# Steps to generate:
# - Clone SDP spack repository
# - spack repo add <REPO>
# - spack env create myenv
# - spack env activate myenv
# - spack add dp3@6.3 arch=x86_64_v2
# - spack concretize -f
# - cd <SPACK_DIR>/var/spack/environments/myenv
# - spack containerize
#
# The last command will generate the Dockerfile and print it to stdout.
# It still needs a couple manual edits that are explained below.

# Build stage with Spack pre-installed and ready to be used
# NOTE: must use v0.23 version because SDP spack repo uses v0.23 Python API
FROM spack/ubuntu-jammy:0.23 AS builder

# NOTE: Must add SDP spack repo first
RUN git clone --depth=1 https://gitlab.com/ska-telescope/sdp/ska-sdp-spack.git /opt/repos/ska-sdp-spack
RUN spack repo add /opt/repos/ska-sdp-spack

# What we want to install and how we want to install it
# is specified in a manifest file (spack.yaml)
# NOTE: Had to update the syntax of the YAML below
# {"install_tree": PATH} must be replaced by {"install_tree": {"root": PATH}}
# NOTE: Adding git lfs and make that are both required by the CI pipeline
RUN mkdir -p /opt/spack-environment && \
set -o noclobber \
&&  (echo spack: \
&&   echo '  # add package specs to the `specs` list' \
&&   echo '  specs:' \
&&   echo '  - dp3@6.3 arch=None-None-x86_64_v2' \
&&   echo '  - git-lfs@3.5.1 arch=None-None-x86_64_v2' \
&&   echo '  - gmake@4.4.1 arch=None-None-x86_64_v2' \
&&   echo '  view: /opt/views/view' \
&&   echo '  concretizer:' \
&&   echo '    unify: true' \
&&   echo '    targets:' \
&&   echo '      granularity: generic' \
&&   echo '  config:' \
&&   echo '    install_tree:' \
&&   echo '      root: /opt/software') > /opt/spack-environment/spack.yaml

# Install the software, remove unnecessary deps
# NOTE: added argument to spack install: -j $(nproc)
RUN cd /opt/spack-environment && spack env activate . && spack install -j $(nproc) --fail-fast && spack gc -y

# Strip all the binaries
RUN find -L /opt/views/view/* -type f -exec readlink -f '{}' \; | \
    xargs file -i | \
    grep 'charset=binary' | \
    grep 'x-executable\|x-archive\|x-sharedlib' | \
    awk -F: '{print $1}' | xargs strip

# Modifications to the environment that are necessary to run
RUN cd /opt/spack-environment && \
    spack env activate --sh -d . > activate.sh

# Bare OS image to run the installed executables
FROM ubuntu:22.04

COPY --from=builder /opt/spack-environment /opt/spack-environment
COPY --from=builder /opt/software /opt/software

# paths.view is a symlink, so copy the parent to avoid dereferencing and duplicating it
COPY --from=builder /opt/views /opt/views

RUN { \
      echo '#!/bin/sh' \
      && echo '.' /opt/spack-environment/activate.sh \
      && echo 'exec "$@"'; \
    } > /entrypoint.sh \
&& chmod a+x /entrypoint.sh \
&& ln -s /opt/views/view /opt/view

ENTRYPOINT [ "/entrypoint.sh" ]
CMD [ "/bin/bash" ]
