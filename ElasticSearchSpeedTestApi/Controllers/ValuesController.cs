using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Elasticsearch.Net;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Nest;

namespace ElasticSearchSpeedTestApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ValuesController : ControllerBase
    {
        private IConfiguration configuration;
        public static string CONNECTION_STRING = string.Empty;
        public static string INDEX_NAME = "elasticdemo";
        public static string INDEX_TYPE = "report4";
      

        public ValuesController(IConfiguration iConfig)
        {
            configuration = iConfig;
            CONNECTION_STRING = configuration.GetSection("ConnectionStr").GetSection("DefaultConnection").Value;
         
        }

        // GET api/values
        [HttpGet]
        public IActionResult Get()
        {
            // var result = GetAllRecords("Snacks");
            // return Ok(result);
            return Ok("test");
        }

        // GET api/values/5
        [HttpGet("{id}")]
        public ActionResult<string> Get(int id)
        {
            return "value";
        }

        // POST api/values
        [HttpPost]
        public IActionResult Post([FromBody] string value)
        {
            // 1.Elastic Search

            //// 2. SQL Search Queries...
            var result = GetAllRecords(value);
            return Ok(result.Count());
        }
        [HttpPost("PostElastic")]
        public IActionResult PostElastic([FromBody] string value)
        {
            // 1.Elastic Search

            var result = ConfigureES(value);
            return Ok(result.Count());


        }
        // PUT api/values/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody] string value)
        {
        }

        // DELETE api/values/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }

        public static List<Sales> ConfigureES(string inputText)
        {
            List<Sales> salesReports = new List<Sales>();

            // 1. Connection URL's elastic search
            var listOfUrls = new Uri[]
            {
                // here we can set multple connectionn URL's...
                 new Uri("http://localhost:9200/")
            };

            StaticConnectionPool connPool = new StaticConnectionPool(listOfUrls);
            ConnectionSettings connSett = new ConnectionSettings(connPool);
            ElasticClient eClient = new ElasticClient(connSett);

            //  var see = eClient.DeleteIndex(INDEX_NAME);

            // check the connection health
            var checkClusterHealth = eClient.ClusterHealth();
            if (checkClusterHealth.ApiCall.Success && checkClusterHealth.IsValid)
            {
                // 2. check the index exist or not 
                var checkResult = eClient.IndexExists(INDEX_NAME);
                if (!checkResult.Exists)
                {
                    // Raise error to Index not avaliable
                }

                // Search All fileds in a documet type 
                //var searchResponse = eClient.Search<GlobalCompany>(s => s
                //.Index(INDEX_NAME)
                //.Type(INDEX_TYPE)
                //.Query(q => q.QueryString(qs => qs.Query(inputText + "*"))));

                // Search particular text field 
                var searchResponse = eClient.Search<Sales>(s => s.Index(INDEX_NAME)
                                                                            .Query(q => q                    
                                                                              .Bool(b => b                     
                                                                                .Should(m => m
                                                                                  .Wildcard(c => c
                                                                                    .Field(f=>f.Region).Value(inputText.ToLower() + "*"))))));



                //var results = eClient.Scroll<Salesreport>("10m", searchResponse.ScrollId);
                while (searchResponse.Documents.Any())
                {
                    var res = searchResponse.Documents;
                    var sds = res.Cast<Sales>();
                    salesReports.AddRange(sds);
                    searchResponse = eClient.Scroll<Sales>("10m", searchResponse.ScrollId);
                }

                if (salesReports.Count > 0)
                {

                }
                else
                {

                }

                //var lastRecordResponse = eClient.Search<Salesreport>(s => s
                //    .Index(INDEX_NAME)
                //    .Type(INDEX_TYPE)
                //    .From(0)
                //    .Size(1).Sort(sr => sr.Descending(f => f.Region)));

                if (searchResponse.ApiCall.Success && searchResponse.IsValid)
                {

                }
                else
                {
                    // fail log the exception further use
                    var exception = searchResponse.OriginalException.ToString();
                    var debugException = searchResponse.DebugInformation.ToString();
                }
            }
            else
            {
                // fail log the exception further use
                var exception = checkClusterHealth.OriginalException.ToString();
                var debugException = checkClusterHealth.DebugInformation.ToString();
            }

            return salesReports;
        }

        public static List<Sales> GetAllRecords(string itemType)
        {
            List<Sales> salesReports = new List<Sales>();

            string sqlQuery = String.Format(@"SELECT * FROM dbo.Sales  where Region like '%{0}%'", itemType);
            using (SqlConnection connection = new SqlConnection(CONNECTION_STRING))
            {
                var result = connection.Query<Sales>(sqlQuery);
                foreach (var item in result)
                {
                    Sales global = new Sales()
                    {
                        Region = item.Region,
                        Country = item.Country,
                        Item_Type=item.Item_Type,
                        Order_Date=item.Order_Date,
                        Order_ID = item.Order_ID,
                        Order_Priority=item.Order_Priority,
                        Sales_Channel=item.Sales_Channel,
                        Ship_Date = item.Ship_Date,
                        Total_Cost=item.Total_Cost,
                        Total_Profit=item.Total_Profit,
                        Total_Revenue=item.Total_Revenue,
                        Units_Sold=item.Units_Sold,
                        Unit_Cost=item.Unit_Cost,
                        Unit_Price = item.Unit_Price
                    };
                    salesReports.Add(global);
                   
                }
                return result.ToList();
            }
        }
    }
    public static class ElasticSearchUtility{}
    public class Sales
    {
        public string Region { get; set; }

        public string Country { get; set; }

        public string Item_Type { get; set; }

        public string Sales_Channel { get; set; }

        public string Order_Priority { get; set; }

        public string Order_Date { get; set; }

        public int Order_ID { get; set; }

        public string Ship_Date { get; set; }

        public string Units_Sold { get; set; }

        public string Unit_Price { get; set; }

        public string Unit_Cost { get; set; }

        public string Total_Revenue { get; set; }

        public decimal Total_Cost { get; set; }

        public string Total_Profit { get; set; }

    }
}
