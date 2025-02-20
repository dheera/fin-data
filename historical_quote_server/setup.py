from setuptools import setup, find_packages

setup(
    name="historical_quote_server",
    version="0.1",
    packages=find_packages(),
    # Optionally, if you want to create command-line scripts,
    # you can define entry points. For example, if you add a main()
    # function in your client.py and server.py:
    #
    # entry_points={
    #     'console_scripts': [
    #         'start-server=historical_quote_server.server:main',
    #         'start-client=historical_quote_server.client:main',
    #     ],
    # },
    install_requires=[],  # List any dependencies here
    description="A minimal package for serving historical quotes.",
    author="Your Name",
)

