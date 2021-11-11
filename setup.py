import setuptools

setuptools.setup(name='feast_examplegen',
                 version='0.1',
                 description='Feast Example Gen',
                 install_requires=['feast', 'tfx>=1.0'],
                 packages=setuptools.find_packages(),
                 zip_safe=False)
