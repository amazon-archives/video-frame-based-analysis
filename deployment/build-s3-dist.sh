#!/usr/bin/env bash
# This assumes all of the OS-level configuration has been completed and git repo has already been cloned
#sudo pip install --upgrade pip
#alias sudo='sudo env PATH=$PATH'
#sudo  pip install --upgrade setuptools
#sudo pip install --upgrade virtualenv

# This script should be run from the repo's deployment directory
# cd deployment
# ./build-s3-dist.sh source-bucket-base-name
# source-bucket-base-name should be the base name for the S3 bucket location where the template will source the Lambda code from.
# The template will append '-[region_name]' to this bucket name.
# For example: ./build-s3-dist.sh solutions
# The template will then expect the source code to be located in the solutions-[region_name] bucket

# Check to see if input has been provided:
if [ -z "$1" ]; then
    echo "Please provide the base source bucket name where the lambda code will eventually reside.\nFor example: ./build-s3-dist.sh solutions"
    exit 1
fi

# Build source
echo "Starting to build distribution"
echo "export deployment_dir=`pwd`"
export deployment_dir=`pwd`
echo "mkdir -p dist"
mkdir -p dist
mkdir -p dist/server
# Copying and modifying template
echo "Updating Lambda code source bucket in template with $1"
cp $deployment_dir/video-frame-based-analysis.template $deployment_dir/dist/
replace="s/%%BUCKET_NAME%%/$1/g"
echo "sed -i '' -e $replace $deployment_dir/dist/video-frame-based-analysis.template"
sed -i '' -e $replace $deployment_dir/dist/video-frame-based-analysis.template
# Copying EC2 instance bootstrap files
echo "Copying EC2 instance bootstrap files"
cd ..
pwd
echo "cd source"
cd source
cp 01-PreProcessService/opt/video-frame-based-analysis/preprocess-service.sh $deployment_dir/dist/server/
cp 01-PreProcessService/etc/init.d/supervisor $deployment_dir/dist/server/
cp 01-PreProcessService/etc/supervisord.conf $deployment_dir/dist/server/
echo "Building Search Video By Photo Lambda functions"
echo "Building 01-SVBP_rekognition_core"
cd 01-SVBP_rekognition_core
zip -q -r9 $deployment_dir/dist/01-SVBP_rekognition_core.zip lambda_function.py ../../NOTICE.txt ../../LICENSE.txt
cd ..
cd 99-rekog_collection_controller
zip -q -r9 $deployment_dir/dist/01-SVBP_rekognition_core.zip rekog_collection_controller.py
cd ..
echo "Building 02-SVBP_rekognition_worker"
cd 02-SVBP_rekognition_worker
zip -q -r9 $deployment_dir/dist/02-SVBP_rekognition_worker.zip lambda_function.py ../../NOTICE.txt ../../LICENSE.txt
cd ..
echo "Building 03-SVBP_rekognition_ddb_stream"
cd 03-SVBP_rekognition_ddb_stream
zip -q -r9 $deployment_dir/dist/03-SVBP_rekognition_ddb_stream.zip lambda_function.py ../../NOTICE.txt ../../LICENSE.txt
cd ..
echo "Building 04-SVBP_rekognition_iot"
cd 04-SVBP_rekognition_iot
zip -q -r9 $deployment_dir/dist/04-SVBP_rekognition_iot.zip lambda_function.py ../../NOTICE.txt ../../LICENSE.txt
cd ..
echo "Building Video Analysis Lambda funcitons"
echo "Building 03-RVA_IoT_publish_message_function"
cd 03-RVA_IoT_publish_message_function
zip -q -r9 $deployment_dir/dist/03-RVA_IoT_publish_message_function.zip lambda_function.py ../../NOTICE.txt ../../LICENSE.txt
cd ..
echo "Building 05-RVA_process_dynamodbstream_function"
cd 05-RVA_process_dynamodbstream_function
zip -q -r9 $deployment_dir/dist/05-RVA_process_dynamodbstream_function.zip lambda_function.py ../../NOTICE.txt ../../LICENSE.txt
cd ..
cd 99-rekog_collection_controller
zip -q -r9 $deployment_dir/dist/05-RVA_process_dynamodbstream_function.zip rekog_collection_controller.py
cd ..
echo "Building 06-RVA_process_photos_function"
cd 06-RVA_process_photos_function
zip -q -r9 $deployment_dir/dist/06-RVA_process_photos_function.zip lambda_function.py ../../NOTICE.txt ../../LICENSE.txt
cd ..
cd 99-rekog_collection_controller
zip -q -r9 $deployment_dir/dist/06-RVA_process_photos_function.zip rekog_collection_controller.py
cd ..
echo "Building final Label creating Lambda function"
echo "Building 11-Prepare_label_timeline"
cd 11-Prepare_label_timeline
zip -q -r9 $deployment_dir/dist/11-Prepare_label_timeline.zip lambda_function.py ../../NOTICE.txt ../../LICENSE.txt
cd ..
echo "Building 99-metrics"
cd 99-metrics
zip -q -r9 $deployment_dir/dist/99-metrics.zip lambda_function.py logger.py notify.py ../../NOTICE.txt ../../LICENSE.txt
cd ..

echo "Creating Lambda functions complete"
cd $deployment_dir
