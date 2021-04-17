import os
import shutil
from setuptools import setup, find_packages


OUTPUT_DIR = 'Output'


if __name__ == "__main__":
    setup(
        name="aggregator-data-processing-pipeline",
        packages=['Preprocessing','MDAFollet','ReadWriteData','AttributeGenerators','StagingDataGenerators'],
        version="1.0",
        script_args=['--quiet', 'bdist_egg'], # to create egg-file only
    )

    egg_name = os.listdir('dist')[0]

    os.rename(
        os.path.join('dist', egg_name),
        os.path.join(OUTPUT_DIR, egg_name)
    )

    shutil.rmtree('build')
    shutil.rmtree('dist')
    shutil.rmtree('aggregator_data_processing_pipeline.egg-info')