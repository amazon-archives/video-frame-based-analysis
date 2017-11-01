#!/bin/bash -x

cd /opt/video-frame-based-analysis/
mkdir videos images
rm -Rf videos/* images/*

# Parameter section
FFMPEG_FRAMES_PER_SECOND=1
SQS_MAX_NUMBER_OF_MESSAGES=1
SQS_WAIT_TIME_SECONDS=20

# Constants section
INSTANCE_ID=`curl http://169.254.169.254/latest/meta-data/instance-id`
EC2_AVAIL_ZONE=`curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone`
EC2_REGION="`echo \"$EC2_AVAIL_ZONE\" | sed -e 's:\([0-9][0-9]*\)[a-z]*\$:\\1:'`"
QUEUE_URL=$(aws sqs get-queue-url --queue-name "RVA-VIDEO-PROCESS-IN"  --region $EC2_REGION --output text)
BASE_PATH=/opt/video-frame-based-analysis/tmp

function log {
    echo "[$(date +%Y-%m-%dT%H:%M:%S) $INSTANCE_ID] - $1"
}

# Find the proper batch size according to the number of frames
function find_batch_size {
    if [ $1 -lt 1800 ] ; then
        return 20
    elif [ $1 -lt 3600 ] ; then
        return 40
    elif [ $1 -lt 7200 ] ; then
        return 60
    else
        return 80
    fi
}

while :
do
  	QUEUE_MESSAGE=$(aws sqs receive-message --queue-url $QUEUE_URL --region $EC2_REGION --max-number-of-messages $SQS_MAX_NUMBER_OF_MESSAGES --wait-time-seconds $SQS_WAIT_TIME_SECONDS --query "Messages[0].{Body:Body, ReceiptHandle:ReceiptHandle}")

  	if [ "$QUEUE_MESSAGE" != "null" ] ; then
  	    # Extract SQS meaningful values
  	    BUCKET_NAME=$(echo $QUEUE_MESSAGE | jq -r '.Body | fromjson | .Records[0].s3.bucket.name')
  		OBJECT_KEY=$(echo $QUEUE_MESSAGE | jq -r '.Body | fromjson | .Records[0].s3.object.key')
		RECEIPT_HANDLE=$(echo $QUEUE_MESSAGE | jq -r '.ReceiptHandle')

        log "Processing video '$OBJECT_KEY'"

		# Update processing status before processing
		IOT_TOPIC=$(aws s3api head-object --bucket $BUCKET_NAME --key $OBJECT_KEY | jq -r '.Metadata.topic')
		aws lambda invoke --invocation-type Event --function-name <iot_publish_function> --region $EC2_REGION --payload "{\"topic\": \"$IOT_TOPIC\", \"type\": \"status\", \"payload\": {\"message\": \"Extracting frames from video\", \"percentage\": 40}}" /dev/null

		# Create the required folder structure and download video from S3
		FILE_IDENTIFIER=$(basename $OBJECT_KEY)
		VIDEO_PATH=$(dirname $OBJECT_KEY)
		IMAGE_PATH=images/$FILE_IDENTIFIER
		mkdir -p $VIDEO_PATH $IMAGE_PATH
		aws s3 cp s3://$BUCKET_NAME/$OBJECT_KEY $VIDEO_PATH/

		# Extract thumbnails from video and upload it back to S3
		IMAGE_PREFIX=$IMAGE_PATH/$FILE_IDENTIFIER

		ffmpeg -i $OBJECT_KEY -vf "fps=$FFMPEG_FRAMES_PER_SECOND,showinfo" $IMAGE_PREFIX-%05d.jpg

		# Update processing status after processing
		aws lambda invoke --invocation-type Event --function-name <iot_publish_function> --region $EC2_REGION --payload "{\"topic\": #\"$IOT_TOPIC\", \"type\": \"status\", \"payload\": {\"message\": \"Uploading frames\", \"percentage\": 50}}" /dev/null

		LIST_OF_IMAGES=( $(find $IMAGE_PATH -maxdepth 1 -type f) )
        FRAMES_EXTRACTED=`find $IMAGE_PATH -maxdepth 1 -type f | wc -l`
        TOTAL_FRAMES=0
		FILE_COUNTER=0
		SUBSET_OF_IMAGES=''
        INVENTORY_FILENAME=1

        find_batch_size $FRAMES_EXTRACTED
        BATCH_SIZE=$?

        # Creates batches metadata (.txt) with frames to be processed
		for IDX in `seq 0 $((${#LIST_OF_IMAGES[@]}-1))`
		do
			TIME_REF=`echo ${LIST_OF_IMAGES[$IDX]} | sed -r 's/.*-([0-9]+).jpg/\1/g' | sed 's/^0*//'`
			TIME=`echo "scale=2;$FFMPEG_FRAMES_PER_SECOND*$TIME_REF*1000" | bc`

			SUBSET_OF_IMAGES="$SUBSET_OF_IMAGES ${LIST_OF_IMAGES[$IDX]}:$TIME"

			FILE_COUNTER=$((FILE_COUNTER+1))
            TOTAL_FRAMES=$((TOTAL_FRAMES+1))

			if [[ "$FILE_COUNTER" -eq $BATCH_SIZE ]] || [[ "$IDX" -eq $((${#LIST_OF_IMAGES[@]}-1)) ]]
			then
				echo $SUBSET_OF_IMAGES > "$IMAGE_PATH/$INVENTORY_FILENAME.txt"
				SUBSET_OF_IMAGES=''
				FILE_COUNTER=0
                INVENTORY_FILENAME=`expr $INVENTORY_FILENAME + 1`
			fi
		done

    # Uploaded all files to S3
    aws s3 sync $IMAGE_PATH/ s3://$BUCKET_NAME/$IMAGE_PATH/ --sse AES256

		# Generating process item which will be put into DynamoDB in order to keep track of the process
		DYNAMODB_PAYLOAD=$(mktemp --suffix "dynamodb.json")
		echo "{\"Identifier\" : {\"S\": \"$FILE_IDENTIFIER\"}, \"Status\" : {\"S\": \"PROCESSING\"}, \"Topic\" : {\"S\": \"$IOT_TOPIC\"}, \"Parts\" : {\"M\":{" >> $DYNAMODB_PAYLOAD
		LIST_OF_BATCHES=( $(find $IMAGE_PATH -maxdepth 1 -type f -name *.txt) )
		for IDX in `seq 0 $((${#LIST_OF_BATCHES[@]}-1))`
		do
		    echo "\"${LIST_OF_BATCHES[$IDX]}\" : {\"S\": \"PENDING\"}" >> $DYNAMODB_PAYLOAD
			if [[ "$IDX" -ne $((${#LIST_OF_BATCHES[@]}-1)) ]]
			then
		    	    echo "," >> $DYNAMODB_PAYLOAD
			fi
		done
		echo '}}}' >> $DYNAMODB_PAYLOAD
		aws dynamodb put-item --table-name RVA_PROCESS_TABLE --item file://$DYNAMODB_PAYLOAD --return-consumed-capacity TOTAL --region $EC2_REGION


    # Update processing status after processing
		aws lambda invoke --invocation-type Event --function-name <iot_publish_function> --region $EC2_REGION --payload "{\"topic\": \"$IOT_TOPIC\", \"type\": \"status\", \"payload\": {\"message\": \"Analyzing frames\", \"percentage\": 60}}" /dev/null
    aws lambda invoke --invocation-type Event --function-name <metrics_function> --region $EC2_REGION --payload "{ \"Data\": { \"VideosProcessed\": 1, \"FramesProcessed\": $TOTAL_FRAMES }}" /dev/null
    log "$OBJECT_KEY contained $TOTAL_FRAMES frames"

		# Clean up the mess
		aws sqs delete-message --queue-url $QUEUE_URL --receipt-handle $RECEIPT_HANDLE --region $EC2_REGION
		rm -Rf videos/$FILE_IDENTIFIER/* images/$FILE_IDENTIFIER/* $DYNAMODB_PAYLOAD
        rm -Rf $VIDEO_PATH/

        log "Video '$OBJECT_KEY' completed!"
	else
		log "No messages so far"
  	fi
done
