// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * @fileoverview Community Connector for npm package download count data. This
 * can retrieve download count for one or multiple packages from npm by date.
 *
 */
var address = 'address';
var user = 'user';
var userPwd = 'password';
var db = 'db';
var msSqlUrlSyntax = 'jdbc:sqlserver://'
var dbUrl = msSqlUrlSyntax + address + ';databaseName=' + db;

var cc = DataStudioApp.createCommunityConnector();
var DEFAULT_TABLE = 'Arz';

// [START get_config]
// https://developers.google.com/datastudio/connector/reference#getconfig
function getConfig() {

  var config = cc.getConfig();

  var ss = config.newSelectSingle()
    .setId("SelectTable")
    .setName("Select Table")
    .setIsDynamic(true)
  var conn = Jdbc.getConnection(dbUrl, user, userPwd);
  var stmt = conn.createStatement();
  var query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_NAME"
  var results = stmt.executeQuery(query);
  while (results.next()) {
    ss.addOption(config.newOptionBuilder().setLabel(results.getString(1)).setValue(results.getString(1)))
  }
  
  config.newTextInput()
  .setId('serverAdress')
  .setName('server adress')
  .setHelpText('server adress here')
  
  config.newTextInput()
  .setId('user')
  .setName('user to connect database')
  .setHelpText('user here')
  config.newInfo()
  .setId("dbUrl")
  .setText(dbUrl)
  return config.build();
  results.close();
  stmt.close();
}
// [END get_config]

function getFields(request) {
  if(!request) return;
  request.configParams = validateConfig(request.configParams);
  var cc = DataStudioApp.createCommunityConnector();
  var configs = cc.getConfig();
  var fields = cc.getFields();
  var types = cc.FieldType;
  Logger.log("request")
  var conn = Jdbc.getConnection(dbUrl, user, userPwd);
  var stmt = conn.createStatement();
  stmt.setMaxRows(1000);
  var query = "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + request.configParams.SelectTable + "'";
  Logger.log(query)
  var results = stmt.executeQuery(query);
  fields = cc.getFields();
  while (results.next()) {
    var ff;
      var fieldtype = results.getString(2);
      switch(fieldtype)
      {
      case "nvarchar":
      case "char":
      case "nchar":
      case "varchar":
      case "uniqueidentifier":
        fields.newDimension()
        .setId(results.getString(1))
        .setName(results.getString(1))
        .setDescription('The date that this was created')
        .setType(types.TEXT);
        break;
      case "datetime":
        fields.newDimension()
        .setId(results.getString(1))
        .setName(results.getString(1))
        .setDescription('The date that this was created')
        .setType(types.YEAR_MONTH_DAY_SECOND);
        break;
      case "smallint":
      case "int":
      case "money":
      case "bit":
        fields.newMetric()
        .setAggregation(cc.AggregationType.SUM)
        .setId(results.getString(1))
        .setName(results.getString(1))
        .setDescription('The date that this was created')
        .setType(types.NUMBER);
        Logger.log(fieldtype + " NUMBER");
        break;
      default:
        fields.newDimension()
        .setId(results.getString(1))
        .setName(results.getString(1))
        .setDescription('The date that this was created')
        .setType(types.TEXT);      
      }
  }
  return fields;

  results.close();
  stmt.close();
}

function isAdminUser() {
  return true;
}

function getSchema(request) {
  var fields = getFields(request).build();
  return { schema: fields };
}
function responseToRows(request,requestedFields) {
  var row = [];
  var conn = Jdbc.getConnection(dbUrl, user, userPwd);
  var stmt = conn.createStatement();
  stmt.setMaxRows(10);
  var fieldsStr = ""
  requestedFields.asArray().forEach(function (field) {
    if(fieldsStr == "")
      fieldsStr = field.getId();
    else 
      fieldsStr = fieldsStr +","+ field.getId();
  });
  var query = "SELECT "+fieldsStr+" FROM "+request.configParams.SelectTable + ";";
  Logger.log(query)
  var results = stmt.executeQuery(query);
  var values = []
  while (results.next()) {
    var row = []
    requestedFields.asArray().forEach(function (field) {
        return row.push(results.getString(field.getId()));
    });
    values.push({ values: row })
  }
  Logger.log({ values: row });
  return values;
}
function getData(request) {
  if(!request) return;
  request.configParams = validateConfig(request.configParams);
  var requestedFieldIds = request.fields.map(function(field) {Logger.log(request.fields);
    return field.name;
  });
  var requestedFields = getFields(request).forIds(requestedFieldIds);

  Logger.log(Object.prototype.toString.call(requestedFieldIds))

  var rows  = responseToRows(request,requestedFields)

  return {
    schema: requestedFields.build(),
    rows: rows
  };
}
/**
 * Validates config parameters and provides missing values.
 *
 * @param {Object} configParams Config parameters from `request`.
 * @returns {Object} Updated Config parameters.
 */
function validateConfig(configParams) {
  configParams = configParams || {};
  configParams.SelectTable = configParams.SelectTable || DEFAULT_TABLE;
  configParams.SelectTable = configParams.SelectTable
    .split(',')
    .map(function (x) {
      return x.trim();
    })
    .join(',');

  return configParams;
}
