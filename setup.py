import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="sqs_msg_transfer-sathyanarayan", # Replace with your own username
    version="0.0.1",
    author="sathyanarayan",
    author_email="sathyanarayan.mec09@gmail.com",
    description="A Transfer SQS messages from one Queue to another",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/sathyanarayan/qs_msg_transfer",
    packages=setuptools.find_packages(),
    python_requires=">=3.6",
    scripts=["bin/sqsmover"],
    install_requires=["boto3>=1.9.236"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
