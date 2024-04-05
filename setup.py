from setuptools import find_packages, setup


def load_requirements():
    with open("requirements.txt", "r") as f:
        return f.read().splitlines()


setup(
    name="cex-adaptors",
    version="1.0.4",
    packages=find_packages(),
    install_requires=load_requirements(),
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
)
