require 'aws-sdk-core'
require 'csv'
require 'pry'

Aws.config.update({
  region: "us-west-2"
})

table_name = "basic-phenotypes"

dynamodb = Aws::DynamoDB::Client.new

params = {
  table_name: table_name,
  key_schema: [
    {
      attribute_name: "participant",
      key_type: "HASH" # partition key
    },
    {
      attribute_name: "13__weight",
      key_type: "RANGE" #sort key
    }
  ],
  attribute_definitions: [
    {
      attribute_name: "participant",
      attribute_type: "S"
    },
    {
      attribute_name: "13__weight",
      attribute_type: "N"
    },
  ],
  provisioned_throughput: {
    read_capacity_units: 10,
    write_capacity_units: 10
  }
}

begin
  result = dynamodb.create_table(params)
  puts "Created table. Status" +
    result.table_description.table_status;

rescue Aws::DynamoDB::Errors::ServiceError => error
  puts "Unable to create table"
  puts "#{error.message}"
end

CSV.foreach("./data/PGPBasicPhenotypesSurvey.csv", headers: true, header_converters: :symbol, converters: :all) do |row|
  item = row.to_hash

  filtered_item = item.reject do |k,v|
    v.nil?
  end

  params = {
    table_name: table_name,
    item: filtered_item
  }
  begin
    result = dynamodb.put_item(params)
    puts "Added participant: #{row['Participant']}"

  rescue Aws::DynamoDB::Errors::ServiceError => error
    puts "Unable to add participant:"
    puts "#{error.message}"
  end
end
