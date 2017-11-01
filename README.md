Video Frame Based Analysis

To help customers process their existing image library and build collections, AWS offers Video Frame Based Analysis, a solution that combines highly available, trusted AWS Services and the open source tools FFmpeg and Open Source Computer Vision (OpenCV), for faster image processing and conversion. This solution allows customers to seamlessly analyze videos on any platform such as mobile, website and desktop, minimize costs, restrictions for integrating solutions, and the collaboration across many different teams within companies. This is a lightweight and fully managed solution, ready to deploy with image recognition tasks, allowing you to focus on high value application design and development.

## OS/Python Environment Setup
```bash
yum update -y
yum install python-setuptools zip -y
pip install --upgrade pip
pip install --upgrade setuptools
pip install --upgrade boto3
pip install --upgrade virtualenv
pip install pyyaml mock moto requests==2.0 Jinja2==2.8
```

## Building Source Package
```bash
cd deployment
./build-s3-dist.sh source-bucket-base-name
```
source-bucket-base-name should be the base name for the S3 bucket location where the template will source the Lambda code from.
The template will append '-[region_name]' to this value.
For example: ./build-s3-dist.sh solutions
The template will then expect the source code to be located in the solutions-[region_name] bucket

## CF template and Lambda function
Located in deployment/dist


***

Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.
