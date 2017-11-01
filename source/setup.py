import os
from setuptools import setup
from setuptools.command.test import test as TestCommand
import subprocess
import sys

def get_data_files(directory):
    paths = {}
    for (path, directories, filenames) in os.walk(directory):
        for filename in filenames:
            if not path.startswith('source/code/scripts'):
                if path not in paths.keys():
                    paths[path] = []
                paths[path].append(os.path.join(path, filename))
    data_files = []
    for k in paths.keys():
        dest_path = k
        if dest_path.startswith('source/'):
            dest_path = dest_path[len('source/'):]
        if dest_path.startswith('code/'):
            dest_path = dest_path[len('code/'):]
        if not dest_path.startswith('scripts/'):
            data_files.append(('project-skeleton/validation_pipeline/' + dest_path, paths[k]))
    return data_files

class CustomTestCommand(TestCommand):
    description = 'run tests'
    user_options = []

    def run_tests(self):
        self._run([sys.executable, '-m', 'unittest', 'discover', '-s', './source','-p','*_test.py'])

    def _run(self, command):
        try:
            subprocess.check_call(command)
        except subprocess.CalledProcessError as error:
            print('Command failed with exit code', error.returncode)
            sys.exit(error.returncode)

setup(
    name="video_frame_based_analysis_on_aws",
    version="0.1",
    author="AWS Solutions Builder",
    description="Setup package for Video Frame Based Analysis",
    license="Amazon Software License 1.0",
    keywords="aws video regkognition",
    url="https://github.com/awslabs/video-frame-based-analysis/",
    data_files=get_data_files('source'),
    classifiers=[
        "Programming Language :: Python :: 2.7",
        "Natural Language :: English",
        "Operating System :: POSIX :: Linux",
        "License :: Amazon Software License :: 1.0",
    ],
    zip_safe=False,
    install_requires=[
        'boto3'
    ],
    tests_require=[
        'mock',
        'boto3',
        'pyyaml',
        'moto',
        'json'
    ],
    cmdclass={'test': CustomTestCommand}
)
