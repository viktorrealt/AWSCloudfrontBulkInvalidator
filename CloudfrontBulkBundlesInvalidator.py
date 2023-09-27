import sys
import getopt
import boto3
import time


def read_args(argv):
    arg_bundles_list_file = ""
    arg_distribution_id = ""
    arg_prefix_to_remove = ""
    arg_help = "{0} -f <bundles-file> -d <distribution-id> -p <prefix-to-remove>".format(argv[0])

    try:
        opts, args = getopt.getopt(argv[1:], "hf:d:p:", ["help", "bundles-file=",
                                                        "distribution-id=", "prefix-to-remove="])
    except:
        print(arg_help)
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(arg_help)  # print the help message
            sys.exit(2)
        elif opt in ("-f", "--bundles-file"):
            arg_bundles_list_file = arg
        elif opt in ("-d", "--distribution-id"):
            arg_distribution_id = arg
        elif opt in ("-p", "--prefix-to-remove"):
            arg_prefix_to_remove = arg

    def extract_s3_lines(file_path):
        s3_lines = []

        try:
            with open(file_path, 'r') as file:
                for line in file:
                    index = line.find('s3://')
                    if index != -1:
                        s3_lines.append(line[index:].strip())

        except FileNotFoundError:
            print(f"File not found: {file_path}")

        prefix_to_remove = arg_prefix_to_remove  # example "s3://bucket-name/bundles/stage"
        if len(prefix_to_remove) > 0:
            s3_lines_normalized =[s.replace(prefix_to_remove, "") for s in s3_lines]
            return s3_lines_normalized
        else:
            return s3_lines

    file_path = arg_bundles_list_file
    s3_lines = extract_s3_lines(file_path)

    # Make CloudFront invalidation
    def invalidate_cloudfront_paths(distribution_id, paths):
        cloudfront_client = boto3.client('cloudfront')

        try:
            # Create a CloudFront invalidation request
            if len(paths) >= 30:
                batch_size = 30
                print("Len: " + str(len(paths)))
                for i in range(0, len(paths), batch_size):
                    batch = paths[i:i + batch_size]
                    invalidation_response = cloudfront_client.create_invalidation(
                        DistributionId=distribution_id,
                        InvalidationBatch={
                            'Paths': {
                                'Quantity': len(batch),
                                'Items': batch
                            },
                            'CallerReference': str(time.time()).replace(".", "")
                        }
                    )
                    # Get the invalidation ID to check the status
                    invalidation_id = invalidation_response['Invalidation']['Id']
                    print(f"Invalidation request submitted with ID: {invalidation_id}")

                    # Wait for the invalidation to complete
                    waiter = cloudfront_client.get_waiter('invalidation_completed')
                    waiter.wait(
                        DistributionId=distribution_id,
                        Id=invalidation_id,
                        WaiterConfig={
                            'Delay': 15,  # Check every 15 seconds
                            'MaxAttempts': 60  # Wait for up to 15 minutes
                        }
                    )
                    print("Invalidation completed successfully.")

            else:
                invalidation_response = cloudfront_client.create_invalidation(
                    DistributionId=distribution_id,
                    InvalidationBatch={
                        'Paths': {
                            'Quantity': len(paths),
                            'Items': paths
                        },
                        'CallerReference': str(time.time()).replace(".", "")
                    }
                )

                # Get the invalidation ID to check the status
                invalidation_id = invalidation_response['Invalidation']['Id']
                print(f"Invalidation request submitted with ID: {invalidation_id}")

                # Wait for the invalidation to complete
                waiter = cloudfront_client.get_waiter('invalidation_completed')
                waiter.wait(
                    DistributionId=distribution_id,
                    Id=invalidation_id,
                    WaiterConfig={
                        'Delay': 15,  # Check every 15 seconds
                        'MaxAttempts': 60  # Wait for up to 15 minutes
                    }
                )
                print("Invalidation completed successfully.")

        except Exception as e:
            print(f"An error occurred: {e}")

    distribution_id = arg_distribution_id  # Replace with your CloudFront distribution ID
    file_paths_to_invalidate = s3_lines
    invalidate_cloudfront_paths(distribution_id, file_paths_to_invalidate)


if __name__ == "__main__":
    read_args(sys.argv)
