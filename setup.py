from setuptools import setup, find_packages

setup(
    name="gRASPA-job-tracker",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyyaml",
        "pandas",
    ],
    entry_points={
        'console_scripts': [
            'gRASPA-job-tracker=gRASPA_job_tracker.cli:main',
        ],
    },
    author="Your Name",
    author_email="your.email@example.com",
    description="A package for tracking and submitting GRASPA simulation jobs",
    keywords="slurm, gRASPA, simulation, job tracking",
    python_requires=">=3.6",
)
