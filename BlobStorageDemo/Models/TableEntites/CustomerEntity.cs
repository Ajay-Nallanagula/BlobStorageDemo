using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobStorageDemo.Models.TableEntites
{
    class CustomerEntity : TableEntity
    {
        //Table Entity  classes require partitionkey and row key to uniquely identify the record.
        public CustomerEntity(string lastname, string firstname)
        {
            this.PartitionKey = lastname;
            this.RowKey = firstname;
        }

         public CustomerEntity() { }
         public int MyProperty { get; set; } 
         public string Email { get; set; }
         public string Phone { get; set; }
        

    }
}
