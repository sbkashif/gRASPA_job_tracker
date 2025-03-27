from setuptools import setup, find_packages

setup(
    name="gRASPA_job_tracker",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyyaml",
        "pandas",
        "wget",  # For downloading databases
        "tqdm",  # For progress bars
    ],
    entry_points={
        'console_scripts': [
            'gRASPA_job_tracker=gRASPA_job_tracker.cli:main',
        ],
    },
    author="Salman Bin Kashif",
    author_email="salmanbinkashif@gmail.com",
    description="A package for tracking and submitting gRASPA simulation jobs",
    keywords="slurm, gRASPA, simulation, job tracking, batch processing",
    python_requires=">=3.6",
)
