//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace GraphBulkImportSample
{
    using Microsoft.Azure.CosmosDB.BulkExecutor.Graph.Element;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Configuration;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;

    internal sealed class Utils
    {

        /// <summary>
        /// Get the collection if it exists, null if it doesn't.
        /// </summary>
        /// <returns>The requested collection.</returns>
        public static DocumentCollection GetCollectionIfExists(DocumentClient client, string databaseName, string collectionName)
        {
            if (GetDatabaseIfExists(client, databaseName) == null)
            {
                return null;
            }

            return client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(databaseName))
                .Where(c => c.Id == collectionName).AsEnumerable().FirstOrDefault();
        }

        /// <summary>
        /// Get the database if it exists, null if it doesn't.
        /// </summary>
        /// <returns>The requested database.</returns>
        public static Database GetDatabaseIfExists(DocumentClient client, string databaseName)
        {
            return client.CreateDatabaseQuery().Where(d => d.Id == databaseName).AsEnumerable().FirstOrDefault();
        }

        /// <summary>
        /// Create a partitioned collection.
        /// </summary>
        /// <returns>The created collection.</returns>
        public static async Task<DocumentCollection> CreatePartitionedCollectionAsync(DocumentClient client, string databaseName,
            string collectionName, int collectionThroughput)
        {
            PartitionKeyDefinition partitionKey = new PartitionKeyDefinition
            {
                Paths = new Collection<string> { $"/{ConfigurationManager.AppSettings["CollectionPartitionKey"]}" }
            };
            DocumentCollection collection = new DocumentCollection { Id = collectionName, PartitionKey = partitionKey };

            try
            {
                collection = await client.CreateDocumentCollectionAsync(
                    UriFactory.CreateDatabaseUri(databaseName),
                    collection,
                    new RequestOptions { OfferThroughput = collectionThroughput });
            }
            catch (Exception e)
            {
                throw e;
            }

            return collection;
        }

        public static IEnumerable<GremlinEdge> GenerateEdges(long count)
        {
            for (long i = 0; i < count - 1; i++)
            {
                GremlinEdge e = new GremlinEdge(
                    "e" + i,
                    "knows",
                    i.ToString(),
                    (i + 1).ToString(),
                    "vertex",
                    "vertex",
                    i,
                    i + 1);

                e.AddProperty("duration", i);

                yield return e;
            }
        }

        public static IEnumerable<GremlinVertex> GenerateVertices(long count)
        {
            // CosmosDB currently doesn't support documents with id length > 1000
            GremlinVertex vBad = new GremlinVertex(getLongId(), "vertex");
            vBad.AddProperty(ConfigurationManager.AppSettings["CollectionPartitionKey"], 0);
            yield return vBad;

            for (long i = 0; i < count; i++)
            {
                GremlinVertex v = new GremlinVertex(i.ToString(), "vertex");
                v.AddProperty(ConfigurationManager.AppSettings["CollectionPartitionKey"], i);
                v.AddProperty("name1", "name" + i);
                v.AddProperty("name2", i * 2);
                v.AddProperty("name3", i * 3);
                v.AddProperty("name4", i + 100);

                yield return v;
            }
        }

        public class RootObject
        {
            public string vertex1 { get; set; }
            public string edge { get; set; }
            public string vertex2 { get; set; }
            public string Label { get; set; }
            public string Name { get; set; }
            public string pk { get; set; }
            public string sinkpk { get; set; }
        }

        public static IEnumerable<GremlinEdge> GenerateEdgesCustom(long count)
        {
            int counter = 0;
            string dirFile = @"..\..\data\edges";
            foreach (string fileName in Directory.GetFiles(dirFile))

            {
                
                Console.Write(fileName);
                string jsonValue = System.IO.File.ReadAllText(fileName);
                var RootObjects = JsonConvert.DeserializeObject<List<RootObject>>(jsonValue);

                foreach (var rootObject in RootObjects)
                    
                {
                    GremlinEdge e = new GremlinEdge(
                        "e"+ counter+rootObject.vertex1,
                        rootObject.edge,
                        rootObject.vertex1+" "+rootObject.pk,
                        rootObject.vertex2+" "+ rootObject.sinkpk,
                        //rootObject.vertex1,
                        //rootObject.vertex2,
                        "vertex",
                        "vertex",
                        rootObject.pk,
                        rootObject.sinkpk);

                    e.AddProperty("duration", 0);
                    counter++;
                    yield return e;
                    
                }
            }


        }

        public static IEnumerable<GremlinVertex> GenerateVerticesCustom(long count)
        {
            // CosmosDB currently doesn't support documents with id length > 1000
            GremlinVertex vBad = new GremlinVertex(getLongId(), "vertex");
            vBad.AddProperty(ConfigurationManager.AppSettings["CollectionPartitionKey"], 0);
            yield return vBad;

            string dirFile = @"..\..\data\vertices";
            

            foreach (string fileName in Directory.GetFiles(dirFile))
            {
                string jsonValue = System.IO.File.ReadAllText(fileName);
                var RootObjects = JsonConvert.DeserializeObject<List<RootObject>>(jsonValue);
                foreach (var rootObject in RootObjects)
                {
                    //GremlinVertex v = new GremlinVertex(rootObject.Name, "vertex");
                    GremlinVertex v = new GremlinVertex(rootObject.Name + " " + rootObject.pk, "vertex");
                    v.AddProperty(ConfigurationManager.AppSettings["CollectionPartitionKey"], rootObject.pk);
                    //System.Diagnostics.Debug.WriteLine("rootObject.Name: "+ rootObject.Name);
                    v.AddProperty("Name", rootObject.Name);
                    //v.AddProperty("Age", rootObject.Age);

                    yield return v;
                }
            }

        }

        private static string getLongId()
        {
            return new string('1', 2000);
        }
    }
}
