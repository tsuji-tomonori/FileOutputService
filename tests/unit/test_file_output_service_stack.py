import aws_cdk as core
import aws_cdk.assertions as assertions

from file_output_service.file_output_service_stack import FileOutputServiceStack

# example tests. To run these tests, uncomment this file along with the example
# resource in file_output_service/file_output_service_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = FileOutputServiceStack(app, "file-output-service")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
