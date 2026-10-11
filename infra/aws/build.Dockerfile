# Official AL2023 userspace, matching the deployment's glibc baseline.
# https://docs.aws.amazon.com/linux/al2023/ug/container.html
FROM public.ecr.aws/amazonlinux/amazonlinux:2023
RUN dnf install -y gcc gcc-c++ make cmake perl python3 git binutils tar findutils pkgconf-pkg-config \
    && dnf clean all
