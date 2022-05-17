using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Xtensive.Sql;
using Xtensive.Sql.Ddl;
using Xtensive.Sql.Dml;
using Xtensive.Sql.Model;

namespace DO71AlexeyChanges
{
  public class MSSQLQueries
  {
    private const string ConnectionUrl = @"sqlserver://dotest:dotest@ALEXEYKULAKOVPC\MSSQL2019/DO-Tests?MultipleActiveResultSets=True";

    private readonly Catalog Catalog;
    private readonly SqlDriver sqlDriver;
    private readonly ConcurrentDictionary<string, ISqlCompileUnit> queriesCache;



#pragma warning disable IDE0059, CS0219 // Unnecessary assignment of a value

    public string ParameterInWhere()
    {
      const string key = nameof(ParameterInWhere);

      if (!queriesCache.TryGetValue(key, out var query)) {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.AddRange(product["ProductID"], product["Name"], product["ListPrice"]);
        select.Where = product["ListPrice"] > SqlDml.ParameterRef("p1");
        select.OrderBy.Add(product["ListPrice"]);
        
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string ALotOfColumns()
    {
      const string key = nameof(ALotOfColumns);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var salesSchema = Catalog.Schemas["Sales"];
        var personSchema = Catalog.Schemas["Person"];

        var salesOrderHeaderRef = SqlDml.TableRef(salesSchema.Tables["SalesOrderHeader"], "soh");
        var select = SqlDml.Select(salesOrderHeaderRef);

        var customerRef = SqlDml.TableRef(salesSchema.Tables["Customer"], "ctmr");
        select.From = select.From.LeftOuterJoin(customerRef, salesOrderHeaderRef["CustomerID"] == customerRef["CustomerID"]);

        var contactRef = SqlDml.TableRef(personSchema.Tables["Contact"], "ctct");
        select.From = select.From.LeftOuterJoin(contactRef, contactRef["ContactID"] == salesOrderHeaderRef["ContactID"]);

        var salesPersonRef = SqlDml.TableRef(salesSchema.Tables["SalesPerson"], "sp");
        select.From = select.From.LeftOuterJoin(salesPersonRef, salesPersonRef["SalesPersonID"] == salesOrderHeaderRef["SalesPersonID"]);

        var salesTerritoryRef = SqlDml.TableRef(salesSchema.Tables["SalesTerritory"], "st");
        select.From = select.From.LeftOuterJoin(salesTerritoryRef, salesTerritoryRef["TerritoryID"] == salesOrderHeaderRef["TerritoryID"]);

        var billingAddressRef = SqlDml.TableRef(personSchema.Tables["Address"], "bAddress");
        select.From = select.From.LeftOuterJoin(billingAddressRef, billingAddressRef["AddressID"] == salesOrderHeaderRef["BillToAddressID"]);

        var shippingAddressRef = SqlDml.TableRef(personSchema.Tables["Address"], "sAddress");
        select.From = select.From.LeftOuterJoin(shippingAddressRef, shippingAddressRef["AddressID"] == salesOrderHeaderRef["ShipToAddressID"]);

        var shipMethodRef = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["ShipMethod"], "sm");
        select.From = select.From.LeftOuterJoin(shipMethodRef, shipMethodRef["ShipMethodID"] == salesOrderHeaderRef["ShipMethodID"]);

        var creditCardRef = SqlDml.TableRef(salesSchema.Tables["CreditCard"], "cc");
        select.From = select.From.LeftOuterJoin(creditCardRef, creditCardRef["CreditCardID"] == salesOrderHeaderRef["CreditCardID"]);

        var currencyRateRef = SqlDml.TableRef(salesSchema.Tables["CurrencyRate"], "cr");
        select.From = select.From.LeftOuterJoin(currencyRateRef, currencyRateRef["CurrencyRateID"] == salesOrderHeaderRef["CurrencyRateID"]);

        select.Columns.Add(salesOrderHeaderRef["SalesOrderID"], "SalesOrderID");
        select.Columns.Add(salesOrderHeaderRef["RevisionNumber"], "OrderRevisionNumber");
        select.Columns.Add(salesOrderHeaderRef["OrderDate"], "OrderDate");
        select.Columns.Add(salesOrderHeaderRef["DueDate"], "OrderDueDate");
        select.Columns.Add(salesOrderHeaderRef["ShipDate"], "OrderShipDate");
        select.Columns.Add(salesOrderHeaderRef["Status"], "OrderStatus");
        select.Columns.Add(salesOrderHeaderRef["OnlineOrderFlag"], "OnlineOrderFlag");
        select.Columns.Add(salesOrderHeaderRef["SalesOrderNumber"], "SalesOrderNumber");
        select.Columns.Add(salesOrderHeaderRef["PurchaseOrderNumber"], "PurchaseOrderNumber");
        select.Columns.Add(salesOrderHeaderRef["AccountNumber"], "AccountNumber");

        select.Columns.Add(salesOrderHeaderRef["CustomerID"], "CustomerID");
        select.Columns.Add(customerRef["AccountNumber"], "CustomerAccountNumber");
        select.Columns.Add(customerRef["CustomerType"], "CustomerType");
        select.Columns.Add(customerRef["ModifiedDate"], "CustomerModifiedDate");

        select.Columns.Add(salesOrderHeaderRef["ContactID"], "ContactID");
        select.Columns.Add(contactRef["NameStyle"], "ContactNameStyle");
        select.Columns.Add(contactRef["Title"], "ContactTitle");
        select.Columns.Add(contactRef["FirstName"], "ContactFirstName");
        select.Columns.Add(contactRef["MiddleName"], "ContactMiddleName");
        select.Columns.Add(contactRef["LastName"], "ContactLastName");
        select.Columns.Add(contactRef["Suffix"], "ContactSuffix");
        select.Columns.Add(contactRef["EmailAddress"], "ContactEmailAddress");
        select.Columns.Add(contactRef["EmailPromotion"], "ContactEmailPromotion");
        select.Columns.Add(contactRef["Phone"], "ContactPhone");
        select.Columns.Add(contactRef["PasswordHash"], "ContactPasswordHash");
        select.Columns.Add(contactRef["PasswordSalt"], "ContactPasswordSalt");
        select.Columns.Add(contactRef["AdditionalContactInfo"], "AdditionalContactInfo");
        select.Columns.Add(contactRef["ModifiedDate"], "ContactModifiedDate");

        select.Columns.Add(salesOrderHeaderRef["SalesPersonID"], "SalesPersonID");
        select.Columns.Add(salesPersonRef["SalesQuota"], "SalesPersonSalesQuota");
        select.Columns.Add(salesPersonRef["Bonus"], "SalesPersonBonus");
        select.Columns.Add(salesPersonRef["CommissionPct"], "SalesPersonCommissionPct");
        select.Columns.Add(salesPersonRef["SalesYTD"], "SalesPersonSalesYTD");
        select.Columns.Add(salesPersonRef["SalesLastYear"], "SalesPersonSalesLastYear");
        select.Columns.Add(salesPersonRef["ModifiedDate"], "SalesPersonModifiedDate");

        select.Columns.Add(salesOrderHeaderRef["TerritoryID"], "TerritoryID");
        select.Columns.Add(salesTerritoryRef["Name"], "TerritoryName");
        select.Columns.Add(salesTerritoryRef["CountryRegionCode"], "TerritoryCountryRegionCode");
        select.Columns.Add(salesTerritoryRef["Group"], "TerritoryGroup");
        select.Columns.Add(salesTerritoryRef["TaxRate"], "TerritoryTaxRate");
        select.Columns.Add(salesTerritoryRef["SalesYTD"], "TerritorySalesYTD");
        select.Columns.Add(salesTerritoryRef["SalesLastYear"], "TerritorySalesLastYear");
        select.Columns.Add(salesTerritoryRef["CostYTD"], "TerritoryCostYTD");
        select.Columns.Add(salesTerritoryRef["CostLastYear"], "TerritoryCostLastYear");
        select.Columns.Add(salesTerritoryRef["ModifiedDate"], "TerritoryModifiedDate");

        select.Columns.Add(salesOrderHeaderRef["BillToAddressID"], "BillToAddressID");
        select.Columns.Add(billingAddressRef["AddressLine1"], "BillingAddressLine1");
        select.Columns.Add(billingAddressRef["AddressLine2"], "BillingAddressLine2");
        select.Columns.Add(billingAddressRef["City"], "BillingAddressCity");
        select.Columns.Add(billingAddressRef["PostalCode"], "BillingAddressPostalCode");
        select.Columns.Add(billingAddressRef["ModifiedDate"], "BillingAddressModifiedDate");

        select.Columns.Add(salesOrderHeaderRef["ShipToAddressID"], "ShippingAddressID");
        select.Columns.Add(shippingAddressRef["AddressLine1"], "ShppingAddressLine1");
        select.Columns.Add(shippingAddressRef["AddressLine2"], "ShippingAddressLine2");
        select.Columns.Add(shippingAddressRef["City"], "ShippingCity");
        select.Columns.Add(shippingAddressRef["PostalCode"], "ShippingPostalCode");
        select.Columns.Add(shippingAddressRef["ModifiedDate"], "ShippingModifiedDate");

        select.Columns.Add(salesOrderHeaderRef["ShipMethodID"], "ShipMethodID");
        select.Columns.Add(shipMethodRef["Name"], "ShipMethodName");
        select.Columns.Add(shipMethodRef["ShipBase"], "ShipMethodShipBase");
        select.Columns.Add(shipMethodRef["ShipRate"], "ShipMethodShipRate");
        select.Columns.Add(shipMethodRef["ModifiedDate"], "ShipMethodModifiedDate");

        select.Columns.Add(salesOrderHeaderRef["CreditCardID"], "CreditCardID");
        select.Columns.Add(creditCardRef["CardNumber"], "CreditCardNumber");
        select.Columns.Add(creditCardRef["ExpMonth"], "CreditCardExpMonth");
        select.Columns.Add(creditCardRef["ExpYear"], "CreditCardExpYear");
        select.Columns.Add(creditCardRef["ModifiedDate"], "CreditCardModifiedDate");

        select.Columns.Add(salesOrderHeaderRef["CreditCardApprovalCode"], "CreditCardApprovalCode");

        select.Columns.Add(salesOrderHeaderRef["CurrencyRateID"], "CurrencyRateID");
        select.Columns.Add(currencyRateRef["CurrencyRateDate"], "CurrencyRateDate");
        select.Columns.Add(currencyRateRef["FromCurrencyCode"], "CurrencyRateFromCurrencyCode");
        select.Columns.Add(currencyRateRef["ToCurrencyCode"], "CurrencyRateToCurrencyCode");
        select.Columns.Add(currencyRateRef["AverageRate"], "CurrencyRateAverageRate");
        select.Columns.Add(currencyRateRef["EndOfDayRate"], "CurrencyRateEndOfDayRate");
        select.Columns.Add(currencyRateRef["ModifiedDate"], "CurrencyRateModifiedDate");

        select.Columns.Add(salesOrderHeaderRef["SubTotal"], "OrderSubTotal");
        select.Columns.Add(salesOrderHeaderRef["TaxAmt"], "OrderTaxAmt");
        select.Columns.Add(salesOrderHeaderRef["Freight"], "OrderFreight");
        select.Columns.Add(salesOrderHeaderRef["TotalDue"], "OrderTotalDue");
        select.Columns.Add(salesOrderHeaderRef["Comment"], "OrderComment");
        select.Columns.Add(salesOrderHeaderRef["ModifiedDate"], "OrderModifiedDate");

        select.Columns.Add(SqlDml.Literal(10), "Const1");
        select.Columns.Add(SqlDml.Literal(100), "Const2");
        select.Columns.Add(SqlDml.Literal(1000), "Const3");
        select.Columns.Add(SqlDml.Literal(10000), "Const4");
        select.Columns.Add(SqlDml.Literal(100000), "Const5");
        select.Columns.Add(SqlDml.Literal(1000000), "Const6");
        select.Columns.Add(SqlDml.Literal(100000), "Const7");
        select.Columns.Add(SqlDml.Literal(10000), "Const8");
        select.Columns.Add(SqlDml.Literal(1000), "Const9");
        select.Columns.Add(SqlDml.Literal(100), "Const10");
        select.Columns.Add(SqlDml.Literal(10), "Const11");

        query = select;

        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string ALotOfColumnsWithQueryAsSource()
    {
      var key = nameof(ALotOfColumnsWithQueryAsSource);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var salesSchema = Catalog.Schemas["Sales"];
        var personSchema = Catalog.Schemas["Person"];

        var salesOrderHeaderRef = SqlDml.TableRef(salesSchema.Tables["SalesOrderHeader"], "soh");
        var innerSelect = SqlDml.Select(salesOrderHeaderRef);

        var customerRef = SqlDml.TableRef(salesSchema.Tables["Customer"], "ctmr");
        innerSelect.From = innerSelect.From.LeftOuterJoin(customerRef, salesOrderHeaderRef["CustomerID"] == customerRef["CustomerID"]);

        var contactRef = SqlDml.TableRef(personSchema.Tables["Contact"], "ctct");
        innerSelect.From = innerSelect.From.LeftOuterJoin(contactRef, contactRef["ContactID"] == salesOrderHeaderRef["ContactID"]);

        var salesPersonRef = SqlDml.TableRef(salesSchema.Tables["SalesPerson"], "sp");
        innerSelect.From = innerSelect.From.LeftOuterJoin(salesPersonRef, salesPersonRef["SalesPersonID"] == salesOrderHeaderRef["SalesPersonID"]);

        var salesTerritoryRef = SqlDml.TableRef(salesSchema.Tables["SalesTerritory"], "st");
        innerSelect.From = innerSelect.From.LeftOuterJoin(salesTerritoryRef, salesTerritoryRef["TerritoryID"] == salesOrderHeaderRef["TerritoryID"]);

        var billingAddressRef = SqlDml.TableRef(personSchema.Tables["Address"], "bAddress");
        innerSelect.From = innerSelect.From.LeftOuterJoin(billingAddressRef, billingAddressRef["AddressID"] == salesOrderHeaderRef["BillToAddressID"]);

        var shippingAddressRef = SqlDml.TableRef(personSchema.Tables["Address"], "sAddress");
        innerSelect.From = innerSelect.From.LeftOuterJoin(shippingAddressRef, shippingAddressRef["AddressID"] == salesOrderHeaderRef["ShipToAddressID"]);

        var shipMethodRef = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["ShipMethod"], "sm");
        innerSelect.From = innerSelect.From.LeftOuterJoin(shipMethodRef, shipMethodRef["ShipMethodID"] == salesOrderHeaderRef["ShipMethodID"]);

        var creditCardRef = SqlDml.TableRef(salesSchema.Tables["CreditCard"], "cc");
        innerSelect.From = innerSelect.From.LeftOuterJoin(creditCardRef, creditCardRef["CreditCardID"] == salesOrderHeaderRef["CreditCardID"]);

        var currencyRateRef = SqlDml.TableRef(salesSchema.Tables["CurrencyRate"], "cr");
        innerSelect.From = innerSelect.From.LeftOuterJoin(currencyRateRef, currencyRateRef["CurrencyRateID"] == salesOrderHeaderRef["CurrencyRateID"]);

        innerSelect.Columns.Add(salesOrderHeaderRef["SalesOrderID"], "SalesOrderID");
        innerSelect.Columns.Add(salesOrderHeaderRef["RevisionNumber"], "OrderRevisionNumber");
        innerSelect.Columns.Add(salesOrderHeaderRef["OrderDate"], "OrderDate");
        innerSelect.Columns.Add(salesOrderHeaderRef["DueDate"], "OrderDueDate");
        innerSelect.Columns.Add(salesOrderHeaderRef["ShipDate"], "OrderShipDate");
        innerSelect.Columns.Add(salesOrderHeaderRef["Status"], "OrderStatus");
        innerSelect.Columns.Add(salesOrderHeaderRef["OnlineOrderFlag"], "OnlineOrderFlag");
        innerSelect.Columns.Add(salesOrderHeaderRef["SalesOrderNumber"], "SalesOrderNumber");
        innerSelect.Columns.Add(salesOrderHeaderRef["PurchaseOrderNumber"], "PurchaseOrderNumber");
        innerSelect.Columns.Add(salesOrderHeaderRef["AccountNumber"], "AccountNumber");

        innerSelect.Columns.Add(salesOrderHeaderRef["CustomerID"], "CustomerID");
        innerSelect.Columns.Add(customerRef["AccountNumber"], "CustomerAccountNumber");
        innerSelect.Columns.Add(customerRef["CustomerType"], "CustomerType");
        innerSelect.Columns.Add(customerRef["ModifiedDate"], "CustomerModifiedDate");

        innerSelect.Columns.Add(salesOrderHeaderRef["ContactID"], "ContactID");
        innerSelect.Columns.Add(contactRef["NameStyle"], "ContactNameStyle");
        innerSelect.Columns.Add(contactRef["Title"], "ContactTitle");
        innerSelect.Columns.Add(contactRef["FirstName"], "ContactFirstName");
        innerSelect.Columns.Add(contactRef["MiddleName"], "ContactMiddleName");
        innerSelect.Columns.Add(contactRef["LastName"], "ContactLastName");
        innerSelect.Columns.Add(contactRef["Suffix"], "ContactSuffix");
        innerSelect.Columns.Add(contactRef["EmailAddress"], "ContactEmailAddress");
        innerSelect.Columns.Add(contactRef["EmailPromotion"], "ContactEmailPromotion");
        innerSelect.Columns.Add(contactRef["Phone"], "ContactPhone");
        innerSelect.Columns.Add(contactRef["PasswordHash"], "ContactPasswordHash");
        innerSelect.Columns.Add(contactRef["PasswordSalt"], "ContactPasswordSalt");
        innerSelect.Columns.Add(contactRef["AdditionalContactInfo"], "AdditionalContactInfo");
        innerSelect.Columns.Add(contactRef["ModifiedDate"], "ContactModifiedDate");

        innerSelect.Columns.Add(salesOrderHeaderRef["SalesPersonID"], "SalesPersonID");
        innerSelect.Columns.Add(salesPersonRef["SalesQuota"], "SalesPersonSalesQuota");
        innerSelect.Columns.Add(salesPersonRef["Bonus"], "SalesPersonBonus");
        innerSelect.Columns.Add(salesPersonRef["CommissionPct"], "SalesPersonCommissionPct");
        innerSelect.Columns.Add(salesPersonRef["SalesYTD"], "SalesPersonSalesYTD");
        innerSelect.Columns.Add(salesPersonRef["SalesLastYear"], "SalesPersonSalesLastYear");
        innerSelect.Columns.Add(salesPersonRef["ModifiedDate"], "SalesPersonModifiedDate");

        innerSelect.Columns.Add(salesOrderHeaderRef["TerritoryID"], "TerritoryID");
        innerSelect.Columns.Add(salesTerritoryRef["Name"], "TerritoryName");
        innerSelect.Columns.Add(salesTerritoryRef["CountryRegionCode"], "TerritoryCountryRegionCode");
        innerSelect.Columns.Add(salesTerritoryRef["Group"], "TerritoryGroup");
        innerSelect.Columns.Add(salesTerritoryRef["TaxRate"], "TerritoryTaxRate");
        innerSelect.Columns.Add(salesTerritoryRef["SalesYTD"], "TerritorySalesYTD");
        innerSelect.Columns.Add(salesTerritoryRef["SalesLastYear"], "TerritorySalesLastYear");
        innerSelect.Columns.Add(salesTerritoryRef["CostYTD"], "TerritoryCostYTD");
        innerSelect.Columns.Add(salesTerritoryRef["CostLastYear"], "TerritoryCostLastYear");
        innerSelect.Columns.Add(salesTerritoryRef["ModifiedDate"], "TerritoryModifiedDate");

        innerSelect.Columns.Add(salesOrderHeaderRef["BillToAddressID"], "BillToAddressID");
        innerSelect.Columns.Add(billingAddressRef["AddressLine1"], "BillingAddressLine1");
        innerSelect.Columns.Add(billingAddressRef["AddressLine2"], "BillingAddressLine2");
        innerSelect.Columns.Add(billingAddressRef["City"], "BillingAddressCity");
        innerSelect.Columns.Add(billingAddressRef["PostalCode"], "BillingAddressPostalCode");
        innerSelect.Columns.Add(billingAddressRef["ModifiedDate"], "BillingAddressModifiedDate");

        innerSelect.Columns.Add(salesOrderHeaderRef["ShipToAddressID"], "ShippingAddressID");
        innerSelect.Columns.Add(shippingAddressRef["AddressLine1"], "ShippingAddressLine1");
        innerSelect.Columns.Add(shippingAddressRef["AddressLine2"], "ShippingAddressLine2");
        innerSelect.Columns.Add(shippingAddressRef["City"], "ShippingCity");
        innerSelect.Columns.Add(shippingAddressRef["PostalCode"], "ShippingPostalCode");
        innerSelect.Columns.Add(shippingAddressRef["ModifiedDate"], "ShippingModifiedDate");

        innerSelect.Columns.Add(salesOrderHeaderRef["ShipMethodID"], "ShipMethodID");
        innerSelect.Columns.Add(shipMethodRef["Name"], "ShipMethodName");
        innerSelect.Columns.Add(shipMethodRef["ShipBase"], "ShipMethodShipBase");
        innerSelect.Columns.Add(shipMethodRef["ShipRate"], "ShipMethodShipRate");
        innerSelect.Columns.Add(shipMethodRef["ModifiedDate"], "ShipMethodModifiedDate");

        innerSelect.Columns.Add(salesOrderHeaderRef["CreditCardID"], "CreditCardID");
        innerSelect.Columns.Add(creditCardRef["CardNumber"], "CreditCardNumber");
        innerSelect.Columns.Add(creditCardRef["ExpMonth"], "CreditCardExpMonth");
        innerSelect.Columns.Add(creditCardRef["ExpYear"], "CreditCardExpYear");
        innerSelect.Columns.Add(creditCardRef["ModifiedDate"], "CreditCardModifiedDate");

        innerSelect.Columns.Add(salesOrderHeaderRef["CreditCardApprovalCode"], "CreditCardApprovalCode");

        innerSelect.Columns.Add(salesOrderHeaderRef["CurrencyRateID"], "CurrencyRateID");
        innerSelect.Columns.Add(currencyRateRef["CurrencyRateDate"], "CurrencyRateDate");
        innerSelect.Columns.Add(currencyRateRef["FromCurrencyCode"], "CurrencyRateFromCurrencyCode");
        innerSelect.Columns.Add(currencyRateRef["ToCurrencyCode"], "CurrencyRateToCurrencyCode");
        innerSelect.Columns.Add(currencyRateRef["AverageRate"], "CurrencyRateAverageRate");
        innerSelect.Columns.Add(currencyRateRef["EndOfDayRate"], "CurrencyRateEndOfDayRate");
        innerSelect.Columns.Add(currencyRateRef["ModifiedDate"], "CurrencyRateModifiedDate");

        innerSelect.Columns.Add(salesOrderHeaderRef["SubTotal"], "OrderSubTotal");
        innerSelect.Columns.Add(salesOrderHeaderRef["TaxAmt"], "OrderTaxAmt");
        innerSelect.Columns.Add(salesOrderHeaderRef["Freight"], "OrderFreight");
        innerSelect.Columns.Add(salesOrderHeaderRef["TotalDue"], "OrderTotalDue");
        innerSelect.Columns.Add(salesOrderHeaderRef["Comment"], "OrderComment");
        innerSelect.Columns.Add(salesOrderHeaderRef["ModifiedDate"], "OrderModifiedDate");

        innerSelect.Columns.Add(SqlDml.Literal(10), "Const1");
        innerSelect.Columns.Add(SqlDml.Literal(100), "Const2");
        innerSelect.Columns.Add(SqlDml.Literal(1000), "Const3");
        innerSelect.Columns.Add(SqlDml.Literal(10000), "Const4");
        innerSelect.Columns.Add(SqlDml.Literal(100000), "Const5");
        innerSelect.Columns.Add(SqlDml.Literal(1000000), "Const6");
        innerSelect.Columns.Add(SqlDml.Literal(100000), "Const7");
        innerSelect.Columns.Add(SqlDml.Literal(10000), "Const8");
        innerSelect.Columns.Add(SqlDml.Literal(1000), "Const9");
        innerSelect.Columns.Add(SqlDml.Literal(100), "Const10");
        innerSelect.Columns.Add(SqlDml.Literal(10), "Const11");


        var queryRef = SqlDml.QueryRef(innerSelect, "q");

        var outerSelect = SqlDml.Select(queryRef);
        outerSelect.Columns.Add(queryRef["SalesOrderID"], "SalesOrderID");
        outerSelect.Columns.Add(queryRef["OrderRevisionNumber"], "OrderRevisionNumber");
        outerSelect.Columns.Add(queryRef["OrderDate"], "OrderDate");
        outerSelect.Columns.Add(queryRef["OrderDueDate"], "OrderDueDate");
        outerSelect.Columns.Add(queryRef["OrderShipDate"], "OrderShipDate");
        outerSelect.Columns.Add(queryRef["OrderStatus"], "OrderStatus");
        outerSelect.Columns.Add(queryRef["OnlineOrderFlag"], "OnlineOrderFlag");
        outerSelect.Columns.Add(queryRef["SalesOrderNumber"], "SalesOrderNumber");
        outerSelect.Columns.Add(queryRef["PurchaseOrderNumber"], "PurchaseOrderNumber");
        outerSelect.Columns.Add(queryRef["AccountNumber"], "AccountNumber");

        outerSelect.Columns.Add(queryRef["CustomerID"], "CustomerID");
        outerSelect.Columns.Add(queryRef["CustomerAccountNumber"], "CustomerAccountNumber");
        outerSelect.Columns.Add(queryRef["CustomerType"], "CustomerType");
        outerSelect.Columns.Add(queryRef["CustomerModifiedDate"], "CustomerModifiedDate");

        outerSelect.Columns.Add(queryRef["ContactID"], "ContactID");
        outerSelect.Columns.Add(queryRef["ContactNameStyle"], "ContactNameStyle");
        outerSelect.Columns.Add(queryRef["ContactTitle"], "ContactTitle");
        outerSelect.Columns.Add(queryRef["ContactFirstName"], "ContactFirstName");
        outerSelect.Columns.Add(queryRef["ContactMiddleName"], "ContactMiddleName");
        outerSelect.Columns.Add(queryRef["ContactLastName"], "ContactLastName");
        outerSelect.Columns.Add(queryRef["ContactSuffix"], "ContactSuffix");
        outerSelect.Columns.Add(queryRef["ContactEmailAddress"], "ContactEmailAddress");
        outerSelect.Columns.Add(queryRef["ContactEmailPromotion"], "ContactEmailPromotion");
        outerSelect.Columns.Add(queryRef["ContactPhone"], "ContactPhone");
        outerSelect.Columns.Add(queryRef["ContactPasswordHash"], "ContactPasswordHash");
        outerSelect.Columns.Add(queryRef["ContactPasswordSalt"], "ContactPasswordSalt");
        outerSelect.Columns.Add(queryRef["AdditionalContactInfo"], "AdditionalContactInfo");
        outerSelect.Columns.Add(queryRef["ContactModifiedDate"], "ContactModifiedDate");

        outerSelect.Columns.Add(queryRef["SalesPersonID"], "SalesPersonID");
        outerSelect.Columns.Add(queryRef["SalesPersonSalesQuota"], "SalesPersonSalesQuota");
        outerSelect.Columns.Add(queryRef["SalesPersonBonus"], "SalesPersonBonus");
        outerSelect.Columns.Add(queryRef["SalesPersonCommissionPct"], "SalesPersonCommissionPct");
        outerSelect.Columns.Add(queryRef["SalesPersonSalesYTD"], "SalesPersonSalesYTD");
        outerSelect.Columns.Add(queryRef["SalesPersonSalesLastYear"], "SalesPersonSalesLastYear");
        outerSelect.Columns.Add(queryRef["SalesPersonModifiedDate"], "SalesPersonModifiedDate");

        outerSelect.Columns.Add(queryRef["TerritoryID"], "TerritoryID");
        outerSelect.Columns.Add(queryRef["TerritoryName"], "TerritoryName");
        outerSelect.Columns.Add(queryRef["TerritoryCountryRegionCode"], "TerritoryCountryRegionCode");
        outerSelect.Columns.Add(queryRef["TerritoryGroup"], "TerritoryGroup");
        outerSelect.Columns.Add(queryRef["TerritoryTaxRate"], "TerritoryTaxRate");
        outerSelect.Columns.Add(queryRef["TerritorySalesYTD"], "TerritorySalesYTD");
        outerSelect.Columns.Add(queryRef["TerritorySalesLastYear"], "TerritorySalesLastYear");
        outerSelect.Columns.Add(queryRef["TerritoryCostYTD"], "TerritoryCostYTD");
        outerSelect.Columns.Add(queryRef["TerritoryCostLastYear"], "TerritoryCostLastYear");
        outerSelect.Columns.Add(queryRef["TerritoryModifiedDate"], "TerritoryModifiedDate");

        outerSelect.Columns.Add(queryRef["BillToAddressID"], "BillToAddressID");
        outerSelect.Columns.Add(queryRef["BillingAddressLine1"], "BillingAddressLine1");
        outerSelect.Columns.Add(queryRef["BillingAddressLine2"], "BillingAddressLine2");
        outerSelect.Columns.Add(queryRef["BillingAddressCity"], "BillingAddressCity");
        outerSelect.Columns.Add(queryRef["BillingAddressPostalCode"], "BillingAddressPostalCode");
        outerSelect.Columns.Add(queryRef["BillingAddressModifiedDate"], "BillingAddressModifiedDate");

        outerSelect.Columns.Add(queryRef["ShippingAddressID"], "ShippingAddressID");
        outerSelect.Columns.Add(queryRef["ShippingAddressLine1"], "ShippingAddressLine1");
        outerSelect.Columns.Add(queryRef["ShippingAddressLine2"], "ShippingAddressLine2");
        outerSelect.Columns.Add(queryRef["ShippingCity"], "ShippingCity");
        outerSelect.Columns.Add(queryRef["ShippingPostalCode"], "ShippingPostalCode");
        outerSelect.Columns.Add(queryRef["ShippingModifiedDate"], "ShippingModifiedDate");

        outerSelect.Columns.Add(queryRef["ShipMethodID"], "ShipMethodID");
        outerSelect.Columns.Add(queryRef["ShipMethodName"], "ShipMethodName");
        outerSelect.Columns.Add(queryRef["ShipMethodShipBase"], "ShipMethodShipBase");
        outerSelect.Columns.Add(queryRef["ShipMethodShipRate"], "ShipMethodShipRate");
        outerSelect.Columns.Add(queryRef["ShipMethodModifiedDate"], "ShipMethodModifiedDate");

        outerSelect.Columns.Add(queryRef["CreditCardID"], "CreditCardID");
        outerSelect.Columns.Add(queryRef["CreditCardNumber"], "CreditCardNumber");
        outerSelect.Columns.Add(queryRef["CreditCardExpMonth"], "CreditCardExpMonth");
        outerSelect.Columns.Add(queryRef["CreditCardExpYear"], "CreditCardExpYear");
        outerSelect.Columns.Add(queryRef["CreditCardModifiedDate"], "CreditCardModifiedDate");

        outerSelect.Columns.Add(queryRef["CreditCardApprovalCode"], "CreditCardApprovalCode");

        outerSelect.Columns.Add(queryRef["CurrencyRateID"], "CurrencyRateID");
        outerSelect.Columns.Add(queryRef["CurrencyRateDate"], "CurrencyRateDate");
        outerSelect.Columns.Add(queryRef["CurrencyRateFromCurrencyCode"], "CurrencyRateFromCurrencyCode");
        outerSelect.Columns.Add(queryRef["CurrencyRateToCurrencyCode"], "CurrencyRateToCurrencyCode");
        outerSelect.Columns.Add(queryRef["CurrencyRateAverageRate"], "CurrencyRateAverageRate");
        outerSelect.Columns.Add(queryRef["CurrencyRateEndOfDayRate"], "CurrencyRateEndOfDayRate");
        outerSelect.Columns.Add(queryRef["CurrencyRateModifiedDate"], "CurrencyRateModifiedDate");

        outerSelect.Columns.Add(queryRef["OrderSubTotal"], "OrderSubTotal");
        outerSelect.Columns.Add(queryRef["OrderTaxAmt"], "OrderTaxAmt");
        outerSelect.Columns.Add(queryRef["OrderFreight"], "OrderFreight");
        outerSelect.Columns.Add(queryRef["OrderTotalDue"], "OrderTotalDue");
        outerSelect.Columns.Add(queryRef["OrderComment"], "OrderComment");
        outerSelect.Columns.Add(queryRef["OrderModifiedDate"], "OrderModifiedDate");

        outerSelect.Columns.Add(queryRef["Const1"], "Const1");
        outerSelect.Columns.Add(queryRef["Const2"], "Const2");
        outerSelect.Columns.Add(queryRef["Const3"], "Const3");
        outerSelect.Columns.Add(queryRef["Const4"], "Const4");
        outerSelect.Columns.Add(queryRef["Const5"], "Const5");
        outerSelect.Columns.Add(queryRef["Const6"], "Const6");
        outerSelect.Columns.Add(queryRef["Const7"], "Const7");
        outerSelect.Columns.Add(queryRef["Const8"], "Const8");
        outerSelect.Columns.Add(queryRef["Const9"], "Const9");
        outerSelect.Columns.Add(queryRef["Const10"], "Const10");
        outerSelect.Columns.Add(queryRef["Const11"], "Const11");


        query = outerSelect;

        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select001()
    {
      //var nativeSql = "SELECT ProductID, Name, ListPrice "
      //  + "FROM Production.Product "
      //    + "WHERE ListPrice > $40 "
      //      + "ORDER BY ListPrice ASC";

      const string key = nameof(Select001);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.AddRange(product["ProductID"], product["Name"], product["ListPrice"]);
        select.Where = product["ListPrice"] > 40;
        select.OrderBy.Add(product["ListPrice"]);
        select.OrderBy.Add(1);

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select002()
    {
      //var nativeSql = "SELECT * "
      //  + "FROM AdventureWorks.Purchasing.ShipMethod";

      const string key = nameof(Select002);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var purchasing = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["ShipMethod"]);
        var select = SqlDml.Select(purchasing);
        select.Columns.Add(SqlDml.Asterisk);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select003()
    {
      //var nativeSql = "SELECT DISTINCT Sales.Customer.CustomerID, Sales.Store.Name "
      //  + "FROM Sales.Customer JOIN Sales.Store ON "
      //    + "(Sales.Customer.CustomerID = Sales.Store.CustomerID) "
      //      + "WHERE Sales.Customer.TerritoryID = 1";

      const string key = nameof(Select003);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var customer = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Customer"]);
        var store = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Store"]);
        var select = SqlDml.Select(customer);
        select.Distinct = true;
        select.Columns.AddRange(customer["CustomerID"], store["Name"]);
        select.From = select.From.InnerJoin(store, customer["CustomerID"] == store["CustomerID"]);
        select.Where = customer["TerritoryID"] == 1;
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select004()
    {
      //var nativeSql = "SELECT DISTINCT c.CustomerID, s.Name "
      //  + "FROM Sales.Customer AS c "
      //    + "JOIN "
      //      + "Sales.Store AS s "
      //        + "ON ( c.CustomerID = s.CustomerID) "
      //          + "WHERE c.TerritoryID = 1";

      const string key = nameof(Select004);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var customer = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Customer"], "c");
        var store = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Store"], "s");
        var select =
          SqlDml.Select(customer.InnerJoin(store, customer["CustomerID"] == store["CustomerID"]));
        select.Distinct = true;
        select.Columns.AddRange(customer["CustomerID"], store["Name"]);
        select.Where = customer["TerritoryID"] == 1;
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select005()
    {
      //var nativeSql = "SELECT DISTINCT ShipToAddressID, TerritoryID "
      //  + "FROM Sales.SalesOrderHeader "
      //    + "ORDER BY TerritoryID";

      const string key = nameof(Select005);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var salesOrderHeader = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderHeader"]);
        var select = SqlDml.Select(salesOrderHeader);
        select.Distinct = true;
        select.Columns.AddRange(salesOrderHeader["ShipToAddressID"], salesOrderHeader["TerritoryID"]);
        select.OrderBy.Add(salesOrderHeader["TerritoryID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select006()
    {
      var nativeSql = "SELECT TOP 10 SalesOrderID, OrderDate "
        + "FROM Sales.SalesOrderHeader "
          + "WHERE OrderDate < '2007-01-01T01:01:01.012'"
            + "ORDER BY OrderDate DESC";

      const string key = nameof(Select006);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var salesOrderHeader = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderHeader"]);
        var select = SqlDml.Select(salesOrderHeader);
        select.Limit = 10;
        select.Columns.AddRange(salesOrderHeader["SalesOrderID"], salesOrderHeader["OrderDate"]);
        select.Where = salesOrderHeader["OrderDate"] < DateTime.Now;
        select.OrderBy.Add(salesOrderHeader["OrderDate"], false);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select007()
    {
      //var nativeSql = "SELECT e.IDENTITYCOL AS \"Employee ID\", "
      //  + "c.FirstName + ' ' + c.LastName AS \"Employee Name\", "
      //    + "c.Phone "
      //      + "FROM AdventureWorks.HumanResources.Employee e "
      //        + "JOIN AdventureWorks.Person.Contact c "
      //          + "ON e.ContactID = c.ContactID "
      //            + "ORDER BY LastName, FirstName ASC";

      const string key = nameof(Select007);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var employee = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["Employee"], "e");
        var contact = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["Contact"], "c");
        var select =
          SqlDml.Select(employee.InnerJoin(contact, employee["ContactID"] == contact["ContactID"]));
        select.Columns.Add(employee["EmployeeID"], "Employee ID");
        select.Columns.Add(contact["FirstName"] + '.' + contact["LastName"], "Employee Name");
        select.Columns.Add(contact["Phone"]);
        select.OrderBy.Add(contact["LastName"]);
        select.OrderBy.Add(contact["FirstName"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select008()
    {
      //var nativeSql = "SELECT * "
      //  + "FROM Production.Product "
      //    + "ORDER BY Name";

      const string key = nameof(Select008);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.Add(SqlDml.Asterisk);
        select.OrderBy.Add(product["Name"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select009()
    {
      //var nativeSql = "SELECT Name AS \"Product Name\" "
      //  + "FROM Production.Product "
      //    + "ORDER BY Name ASC";

      const string key = nameof(Select009);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.Add(product["Name"], "Product Name");
        select.OrderBy.Add(product["Name"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select010()
    {
      //var nativeSql = "SELECT * "
      //  + "FROM Sales.Customer "
      //    + "ORDER BY CustomerID ASC";

      const string key = nameof(Select010);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var customer = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Customer"]);
        var select = SqlDml.Select(customer);
        select.Columns.Add(SqlDml.Asterisk);
        select.OrderBy.Add(customer["CustomerID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select011()
    {
      //var nativeSql =
      //  "SELECT CustomerID, TerritoryID, AccountNumber, CustomerType, ModifiedDate "
      //    + "FROM Sales.Customer "
      //      + "ORDER BY CustomerID ASC";

      const string key = nameof(Select011);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var customer = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Customer"]);
        var select = SqlDml.Select(customer);
        select.Columns.Add(customer["CustomerID"]);
        select.Columns.Add(customer["TerritoryID"]);
        select.Columns.Add(customer["AccountNumber"]);
        select.Columns.Add(customer["CustomerType"]);
        select.Columns.Add(customer["ModifiedDate"]);
        select.OrderBy.Add(customer["CustomerID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select012()
    {
      //var nativeSql = "SELECT s.UnitPrice, p.* "
      //  + "FROM Production.Product p "
      //    + "JOIN "
      //      + "Sales.SalesOrderDetail s "
      //        + "ON (p.ProductID = s.ProductID) "
      //          + "ORDER BY p.ProductID";

      const string key = nameof(Select012);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"], "p");
        var salesOrderDetail =
          SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderDetail"], "s");
        var select = SqlDml.Select(
          product.InnerJoin(salesOrderDetail, product["ProductID"] == salesOrderDetail["ProductID"]));
        select.Columns.Add(salesOrderDetail["UnitPrice"]);
        select.Columns.Add(product.Asterisk);
        select.OrderBy.Add(product["ProductID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select013()
    {
      //var nativeSql = "SELECT c.FirstName, c.Phone "
      //  + "FROM AdventureWorks.HumanResources.Employee e "
      //    + "JOIN AdventureWorks.Person.Contact c "
      //      + "ON e.ContactID = c.ContactID "
      //        + "ORDER BY FirstName ASC";

      const string key = nameof(Select013);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var employee = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["Employee"], "e");
        var contact = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["Contact"], "c");
        var select =
          SqlDml.Select(employee.InnerJoin(contact, employee["ContactID"] == contact["ContactID"]));
        select.Columns.AddRange(contact["FirstName"], contact["Phone"]);
        select.OrderBy.Add(contact["FirstName"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select014()
    {
      //var nativeSql = "SELECT LastName + ', ' + FirstName AS ContactName "
      //  + "FROM AdventureWorks.Person.Contact "
      //    + "ORDER BY LastName, FirstName ASC";

      const string key = nameof(Select014);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var contact = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["Contact"]);
        var select = SqlDml.Select(contact);
        select.Columns.Add(contact["LastName"] + ", " + contact["FirstName"], "ContactName");
        select.OrderBy.Add(contact["LastName"]);
        select.OrderBy.Add(contact["FirstName"]);

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select015()
    {
      //var nativeSql = "SELECT ROUND( (ListPrice * .9), 2) AS DiscountPrice "
      //  + "FROM Production.Product "
      //    + "WHERE ProductID = 58";

      const string key = nameof(Select015);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.Add(SqlDml.Round(product["ListPrice"] * 0.9, 2), "DiscountPrice");
        select.Where = product["ProductID"] == 58;

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select016()
    {
      //var nativeSql = "SELECT ( CAST(ProductID AS VARCHAR(10)) + ': ' "
      //  + "+ Name ) AS ProductIDName "
      //    + "FROM Production.Product";

      const string key = nameof(Select016);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.Add(
          SqlDml.Cast(product["ProductID"], new SqlValueType("varchar(10)")), "ProductIDName");

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select017()
    {
      //var nativeSql = "SELECT ProductID, Name, "
      //  + "CASE Class "
      //    + "WHEN 'H' THEN ROUND( (ListPrice * .6), 2) "
      //      + "WHEN 'L' THEN ROUND( (ListPrice * .7), 2) "
      //        + "WHEN 'M' THEN ROUND( (ListPrice * .8), 2) "
      //          + "ELSE ROUND( (ListPrice * .9), 2) "
      //            + "END AS DiscountPrice "
      //              + "FROM Production.Product";

      const string key = nameof(Select017);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.AddRange(product["ProductID"], product["Name"]);
        var discountPrice = SqlDml.Case(product["Class"]);
        discountPrice['H'] = SqlDml.Round(product["ListPrice"] * 0.6, 2);
        discountPrice['L'] = SqlDml.Round(product["ListPrice"] * 0.7, 2);
        discountPrice['M'] = SqlDml.Round(product["ListPrice"] * 0.8, 2);
        discountPrice.Else = SqlDml.Round(product["ListPrice"] * 0.6, 2);
        select.Columns.Add(discountPrice, "DiscountPrice");
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select018()
    {
      //var nativeSql = "SELECT Prd.ProductID, Prd.Name, "
      //  + "(SELECT SUM(OD.UnitPrice * OD.OrderQty) "
      //    + "FROM AdventureWorks.Sales.SalesOrderDetail AS OD "
      //      + "WHERE OD.ProductID = Prd.ProductID "
      //        + ") AS SumOfSales "
      //          + "FROM AdventureWorks.Production.Product AS Prd "
      //            + "ORDER BY Prd.ProductID";

      const string key = nameof(Select018);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"], "Prd");
        var select = SqlDml.Select(product);
        select.Columns.AddRange(product["ProductID"], product["Name"]);
        var salesOrderDetail =
          SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderDetail"], "OD");
        var sumOfSales = SqlDml.Select(salesOrderDetail);
        sumOfSales.Columns.Add(SqlDml.Sum(salesOrderDetail["UnitPrice"] * salesOrderDetail["OrderQty"]));
        sumOfSales.Where = salesOrderDetail["ProductID"] == product["ProductID"];
        select.Columns.Add(sumOfSales, "SumOfSales");
        select.OrderBy.Add(product["ProductID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select019()
    {
      //var nativeSql = "SELECT p.ProductID, p.Name, "
      //  + "SUM (p.ListPrice * i.Quantity) AS InventoryValue "
      //    + "FROM AdventureWorks.Production.Product p "
      //      + "JOIN AdventureWorks.Production.ProductInventory i "
      //        + "ON p.ProductID = i.ProductID "
      //          + "GROUP BY p.ProductID, p.Name "
      //            + "ORDER BY p.ProductID";

      const string key = nameof(Select019);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var p = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"], "p");
        var i = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["ProductInventory"], "i");
        var select = SqlDml.Select(p.InnerJoin(i, p["ProductID"] == i["ProductID"]));
        select.Columns.AddRange(p["ProductID"], p["Name"]);
        select.Columns.Add(SqlDml.Sum(p["ListPrice"] * i["Quantity"]), "InventoryValue");
        select.GroupBy.AddRange(p["ProductID"], p["Name"]);
        select.OrderBy.Add(p["ProductID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select020()
    {
      //var nativeSql = "SELECT SalesOrderID, "
      //  + "DATEDIFF(dd, ShipDate, GETDATE() ) AS DaysSinceShipped "
      //    + "FROM AdventureWorks.Sales.SalesOrderHeader "
      //      + "WHERE ShipDate IS NOT NULL";

      const string key = nameof(Select020);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var salesOrderHeader = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderHeader"]);
        var select = SqlDml.Select(salesOrderHeader);
        select.Columns.Add(salesOrderHeader["SalesOrderID"]);
        select.Columns.Add(
          SqlDml.FunctionCall(
            "DATEDIFF", SqlDml.Native("dd"), salesOrderHeader["ShipDate"], SqlDml.CurrentDate()),
          "DaysSinceShipped");
        select.Where = SqlDml.IsNotNull(salesOrderHeader["ShipDate"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select021()
    {
      //var nativeSql = "SELECT SalesOrderID, "
      //  + "DaysSinceShipped = DATEDIFF(dd, ShipDate, GETDATE() ) "
      //    + "FROM AdventureWorks.Sales.SalesOrderHeader "
      //      + "WHERE ShipDate IS NOT NULL";

      const string key = nameof(Select021);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var salesOrderHeader = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderHeader"]);
        var select = SqlDml.Select(salesOrderHeader);
        select.Columns.Add(salesOrderHeader["SalesOrderID"]);
        select.Columns.Add(
          SqlDml.FunctionCall(
            "DATEDIFF", SqlDml.Native("dd"), salesOrderHeader["ShipDate"], SqlDml.CurrentDate()),
          "DaysSinceShipped");
        select.Where = SqlDml.IsNotNull(salesOrderHeader["ShipDate"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select022()
    {
      //var nativeSql = "SELECT SUM(TotalDue) AS \"sum\" "
      //  + "FROM Sales.SalesOrderHeader";

      const string key = nameof(Select022);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderHeader"]);
        var select = SqlDml.Select(product);
        select.Columns.Add(SqlDml.Sum(product["TotalDue"]), "sum");
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select023()
    {
      //var nativeSql = "SELECT DISTINCT ProductID "
      //  + "FROM Production.ProductInventory";

      const string key = nameof(Select023);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var productInventory =
        SqlDml.TableRef(Catalog.Schemas["Production"].Tables["ProductInventory"]);
        var select = SqlDml.Select(productInventory);
        select.Distinct = true;
        select.Columns.Add(productInventory["ProductID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select024()
    {
      //var nativeSql = "SELECT Cst.CustomerID, St.Name, Ord.ShipDate, Ord.Freight "
      //  + "FROM AdventureWorks.Sales.Store AS St "
      //    + "JOIN AdventureWorks.Sales.Customer AS Cst "
      //      + "ON St.CustomerID = Cst.CustomerID "
      //        + "JOIN AdventureWorks.Sales.SalesOrderHeader AS Ord "
      //          + "ON Cst.CustomerID = Ord.CustomerID";

      const string key = nameof(Select024);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var st = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Store"], "St");
        var cst = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Customer"], "Cst");
        var ord = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderHeader"], "Ord");
        var select = SqlDml.Select(st);
        select.From = select.From.InnerJoin(cst, st["CustomerID"] == cst["CustomerID"]);
        select.From = select.From.InnerJoin(ord, cst["CustomerID"] == ord["CustomerID"]);
        select.Columns.AddRange(cst["CustomerID"], st["Name"], ord["ShipDate"], ord["Freight"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select025()
    {
      //var nativeSql = "SELECT c.CustomerID, s.Name "
      //  + "FROM Sales.Customer AS c "
      //    + "JOIN Sales.Store AS s "
      //      + "ON c.CustomerID = s.CustomerID";

      const string key = nameof(Select025);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var c = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Customer"], "c");
        var s = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Store"], "s");
        var select = SqlDml.Select(c.InnerJoin(s, c["CustomerID"] == s["CustomerID"]));
        select.Columns.AddRange(c["CustomerID"], s["Name"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select026()
    {
      //var nativeSql = "SELECT c.CustomerID, s.Name "
      //  + "FROM AdventureWorks.Sales.Customer c "
      //    + "JOIN AdventureWorks.Sales.Store s "
      //      + "ON s.CustomerID = c.CustomerID "
      //        + "WHERE c.TerritoryID = 1";

      const string key = nameof(Select026);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var c = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Customer"], "c");
        var s = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Store"], "s");
        var select = SqlDml.Select(c.InnerJoin(s, c["CustomerID"] == s["CustomerID"]));
        select.Columns.AddRange(c["CustomerID"], s["Name"]);
        select.Where = c["TerritoryID"] == 1;
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select027()
    {
      //var nativeSql = "SELECT OrdD1.SalesOrderID AS OrderID, "
      //  + "SUM(OrdD1.OrderQty) AS \"Units Sold\", "
      //    + "SUM(OrdD1.UnitPrice * OrdD1.OrderQty) AS Revenue "
      //      + "FROM Sales.SalesOrderDetail AS OrdD1 "
      //        + "WHERE OrdD1.SalesOrderID in (SELECT OrdD2.SalesOrderID "
      //          + "FROM Sales.SalesOrderDetail AS OrdD2 "
      //            + "WHERE OrdD2.UnitPrice > $100) "
      //              + "GROUP BY OrdD1.SalesOrderID "
      //                + "HAVING SUM(OrdD1.OrderQty) > 100";

      const string key = nameof(Select027);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var ordD1 = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderDetail"], "OrdD1");
        var ordD2 = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderDetail"], "OrdD2");
        var subSelect = SqlDml.Select(ordD2);
        subSelect.Columns.Add(ordD2["SalesOrderID"]);
        subSelect.Where = ordD2["SalesOrderID"] > 100;
        var select = SqlDml.Select(ordD1);
        select.Columns.Add(ordD1["SalesOrderID"], "OrderID");
        select.Columns.Add(SqlDml.Sum(ordD1["OrderQty"]), "Units Sold");
        select.Columns.Add(SqlDml.Sum(ordD1["UnitPrice"] * ordD1["OrderQty"]), "Revenue");
        select.Where = SqlDml.In(ordD1["SalesOrderID"], subSelect);
        select.GroupBy.Add(ordD1["SalesOrderID"]);
        select.Having = SqlDml.Sum(ordD1["OrderQty"]) > 100;
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select028()
    {
      //var nativeSql = "SELECT ProductID, Name "
      //  + "FROM AdventureWorks.Production.Product "
      //    + "WHERE Class = 'H' "
      //      + "ORDER BY ProductID";

      const string key = nameof(Select028);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.AddRange(product["ProductID"], product["Name"]);
        select.Where = product["Class"] == 'H';
        select.OrderBy.Add(product["ProductID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select029()
    {
      //var nativeSql = "SELECT ProductID, Name "
      //  + "FROM AdventureWorks.Production.Product "
      //    + "WHERE ListPrice BETWEEN 100 and 500 "
      //      + "ORDER BY ListPrice";

      const string key = nameof(Select029);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.AddRange(product["ProductID"], product["Name"]);
        select.Where = SqlDml.Between(product["ListPrice"], 100, 500);
        select.OrderBy.Add(product["ListPrice"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select030()
    {
      //var nativeSql = "SELECT ProductID, Name "
      //  + "FROM AdventureWorks.Production.Product "
      //    + "WHERE Color IN ('Multi', 'Silver') "
      //      + "ORDER BY ProductID";

      const string key = nameof(Select030);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.AddRange(product["ProductID"], product["Name"]);
        select.Where = SqlDml.In(product["Color"], SqlDml.Row("Multi", "Silver"));
        select.OrderBy.Add(product["ProductID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select031()
    {
      //var nativeSql = "SELECT ProductID, Name "
      //  + "FROM AdventureWorks.Production.Product "
      //    + "WHERE Name LIKE 'Ch%' "
      //      + "ORDER BY ProductID";

      const string key = nameof(Select031);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.AddRange(product["ProductID"], product["Name"]);
        select.Where = SqlDml.Like(product["Name"], "Ch%");
        select.OrderBy.Add(product["ProductID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select032()
    {
      //var nativeSql = "SELECT s.Name "
      //  + "FROM AdventureWorks.Sales.Customer c "
      //    + "JOIN AdventureWorks.Sales.Store s "
      //      + "ON c.CustomerID = s.CustomerID "
      //        + "WHERE s.SalesPersonID IS NOT NULL "
      //          + "ORDER BY s.Name";

      const string key = nameof(Select032);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var c = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Customer"], "c");
        var s = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Store"], "s");
        var select = SqlDml.Select(c.InnerJoin(s, c["CustomerID"] == s["CustomerID"]));
        select.Columns.Add(s["Name"]);
        select.Where = SqlDml.IsNotNull(s["SalesPersonID"]);
        select.OrderBy.Add(s["Name"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select033()
    {
      var nativeSql = "SELECT OrdD1.SalesOrderID, OrdD1.ProductID "
        + "FROM Sales.SalesOrderDetail OrdD1 "
          + "WHERE OrdD1.OrderQty > ALL "
            + "(SELECT OrdD2.OrderQty "
              +
              "       FROM Sales.SalesOrderDetail OrdD2 JOIN Production.Product Prd "
                + "ON OrdD2.ProductID = Prd.ProductID "
                  + "WHERE Prd.Class = 'H')";

      const string key = nameof(Select033);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var ordD1 = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderDetail"], "OrdD1");
        var ordD2 = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderDetail"], "OrdD2");
        var prd = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"], "Prd");
        var subSelect = SqlDml.Select(ordD2.InnerJoin(prd, ordD2["ProductID"] == prd["ProductID"]));
        subSelect.Columns.Add(ordD2["OrderQty"]);
        subSelect.Where = prd["Class"] == 'H';
        var select = SqlDml.Select(ordD1);
        select.Columns.AddRange(ordD1["SalesOrderID"], ordD1["ProductID"]);
        select.Where = ordD1["OrderQty"] > SqlDml.All(subSelect);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select034()
    {
      //var nativeSql = "SELECT ProductID, Name "
      //  + "FROM AdventureWorks.Production.Product "
      //    + "WHERE ListPrice < 500 "
      //      + "OR (Class = 'L' AND ProductLine = 'S')";

      const string key = nameof(Select034);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.AddRange(product["ProductID"], product["Name"]);
        select.Where = product["ListPrice"] < 500 ||
          (product["Class"] == 'L' && product["ProductLine"] == 'S');
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select035()
    {
      //var nativeSql = "SELECT Name "
      //  + "FROM AdventureWorks.Production.Product "
      //    + "WHERE ListPrice > $50.00";

      const string key = nameof(Select035);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.Add(product["Name"]);
        select.Where = product["ListPrice"] > 50;
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select036()
    {
      //var nativeSql = "SELECT ProductID, Name "
      //  + "FROM AdventureWorks.Production.Product "
      //    + "WHERE ListPrice BETWEEN 15 AND 25";

      const string key = nameof(Select036);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.AddRange(product["ProductID"], product["Name"]);
        select.Where = SqlDml.Between(product["ListPrice"], 15, 25);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select037()
    {
      //var nativeSql = "SELECT ProductID, Name "
      //  + "FROM AdventureWorks.Production.Product "
      //    + "WHERE ListPrice = 15 OR ListPrice = 25";

      const string key = nameof(Select037);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.AddRange(product["ProductID"], product["Name"]);
        select.Where = product["ListPrice"] == 15 || product["ListPrice"] == 25;
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select038()
    {
      //var nativeSql = "SELECT ProductID, Name "
      //  + "FROM AdventureWorks.Production.Product "
      //    + "WHERE ListPrice > 15 AND ListPrice < 25";

      const string key = nameof(Select038);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.AddRange(product["ProductID"], product["Name"]);
        select.Where = product["ListPrice"] > 15 && product["ListPrice"] < 25;
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select039()
    {
      //var nativeSql = "SELECT ProductID, Name "
      //  + "FROM AdventureWorks.Production.Product "
      //    + "WHERE ListPrice NOT BETWEEN 15 AND 25";

      const string key = nameof(Select039);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.AddRange(product["ProductID"], product["Name"]);
        select.Where = !SqlDml.Between(product["ListPrice"], 15, 25);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select040()
    {
      //var nativeSql = "SELECT ProductID, Name "
      //  + "FROM AdventureWorks.Production.Product "
      //    + "WHERE ProductSubcategoryID = 12 OR ProductSubcategoryID = 14 "
      //      + "OR ProductSubcategoryID = 16";

      const string key = nameof(Select040);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.AddRange(product["ProductID"], product["Name"]);
        select.Where = product["ProductSubcategoryID"] == 12 || product["ProductSubcategoryID"] == 14 ||
          product["ProductSubcategoryID"] == 16;
        query = select;

        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select041()
    {
      //var nativeSql = "SELECT ProductID, Name "
      //  + "FROM AdventureWorks.Production.Product "
      //    + "WHERE ProductSubcategoryID IN (12, 14, 16)";

      const string key = nameof(Select041);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.AddRange(product["ProductID"], product["Name"]);
        select.Where = SqlDml.In(product["ProductSubcategoryID"], SqlDml.Row(12, 14, 16));
        query = select;

        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select042()
    {
      //var nativeSql = "SELECT DISTINCT Name "
      //  + "FROM Production.Product "
      //    + "WHERE ProductModelID IN "
      //      + "(SELECT ProductModelID "
      //        + "FROM Production.ProductModel "
      //          + "WHERE Name = 'Long-sleeve logo jersey');";

      const string key = nameof(Select042);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var productModel = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["ProductModel"]);
        var subSelect = SqlDml.Select(productModel);
        subSelect.Columns.Add(productModel["ProductModelID"]);
        subSelect.Where = productModel["Name"] == "Long-sleeve logo jersey";
        var select = SqlDml.Select(product);
        select.Distinct = true;
        select.Columns.Add(product["Name"]);
        select.Where = SqlDml.In(product["ProductModelID"], subSelect);
        query = select;

        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select043()
    {
      //var nativeSql = "SELECT DISTINCT Name "
      //  + "FROM Production.Product "
      //    + "WHERE ProductModelID NOT IN "
      //      + "(SELECT ProductModelID "
      //        + "FROM Production.ProductModel "
      //          + "WHERE Name = 'Long-sleeve logo jersey');";

      const string key = nameof(Select043);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var productModel = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["ProductModel"]);
        var subSelect = SqlDml.Select(productModel);
        subSelect.Columns.Add(productModel["ProductModelID"]);
        subSelect.Where = productModel["Name"] == "Long-sleeve logo jersey";
        var select = SqlDml.Select(product);
        select.Distinct = true;
        select.Columns.Add(product["Name"]);
        select.Where = SqlDml.NotIn(product["ProductModelID"], subSelect);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select044()
    {
      //var nativeSql = "SELECT Phone "
      //  + "FROM AdventureWorks.Person.Contact "
      //    + "WHERE Phone LIKE '415%'";

      const string key = nameof(Select044);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var contact = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["Contact"]);
        var select = SqlDml.Select(contact);
        select.Columns.Add(contact["Phone"]);
        select.Where = SqlDml.Like(contact["Phone"], "415%");
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select045()
    {
      //var nativeSql = "SELECT Phone "
      //  + "FROM AdventureWorks.Person.Contact "
      //    + "WHERE Phone NOT LIKE '415%'";

      const string key = nameof(Select045);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var contact = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["Contact"]);
        var select = SqlDml.Select(contact);
        select.Columns.Add(contact["Phone"]);
        select.Where = !SqlDml.Like(contact["Phone"], "415%");
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select046()
    {
      //var nativeSql = "SELECT Phone "
      //  + "FROM Person.Contact "
      //    + "WHERE Phone LIKE '415%' and Phone IS NOT NULL";

      const string key = nameof(Select046);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var contact = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["Contact"]);
        var select = SqlDml.Select(contact);
        select.Columns.Add(contact["Phone"]);
        select.Where = SqlDml.Like(contact["Phone"], "415%") && SqlDml.IsNotNull(contact["Phone"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select047()
    {
      //var nativeSql = "SELECT Phone "
      //  + "FROM Person.Contact "
      //    + "WHERE Phone LIKE '%5/%%' ESCAPE '/'";

      const string key = nameof(Select047);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var contact = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["Contact"]);
        var select = SqlDml.Select(contact);
        select.Columns.Add(contact["Phone"]);
        select.Where = SqlDml.Like(contact["Phone"], "%5/%%", '/');
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select048()
    {
      //var nativeSql = "SELECT ProductID, Name, Color "
      //  + "FROM AdventureWorks.Production.Product "
      //    + "WHERE Color IS NULL";

      const string key = nameof(Select048);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.AddRange(product["ProductID"], product["Name"], product["Color"]);
        select.Where = SqlDml.IsNull(product["Color"]);

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select049()
    {
      //var nativeSql = "SELECT CustomerID, AccountNumber, TerritoryID "
      //  + "FROM AdventureWorks.Sales.Customer "
      //    + "WHERE TerritoryID IN (1, 2, 3) "
      //      + "OR TerritoryID IS NULL";

      const string key = nameof(Select049);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var customer = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Customer"]);
        var select = SqlDml.Select(customer);
        select.Columns.AddRange(
          customer["CustomerID"], customer["AccountNumber"], customer["TerritoryID"]);
        select.Where = SqlDml.In(customer["TerritoryID"], SqlDml.Row(1, 2, 3)) ||
          SqlDml.IsNull(customer["TerritoryID"]);

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select050()
    {
      //var nativeSql = "SELECT CustomerID, AccountNumber, TerritoryID "
      //  + "FROM AdventureWorks.Sales.Customer "
      //    + "WHERE TerritoryID = NULL";

      const string key = nameof(Select050);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var customer = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Customer"]);
        var select = SqlDml.Select(customer);
        select.Columns.AddRange(
          customer["CustomerID"], customer["AccountNumber"], customer["TerritoryID"]);
        select.Where = customer["TerritoryID"] == SqlDml.Null;
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select051()
    {
      //var nativeSql = "SELECT CustomerID, Name "
      //  + "FROM AdventureWorks.Sales.Store "
      //    + "WHERE CustomerID LIKE '1%' AND Name LIKE N'Bicycle%'";

      const string key = nameof(Select051);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var store = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Store"]);
        var select = SqlDml.Select(store);
        select.Columns.AddRange(store["CustomerID"], store["Name"]);
        select.Where = SqlDml.Like(store["CustomerID"], "1%") && SqlDml.Like(store["Name"], "Bicycle%");
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select052()
    {
      //var nativeSql = "SELECT CustomerID, Name "
      //  + "FROM AdventureWorks.Sales.Store "
      //    + "WHERE CustomerID LIKE '1%' OR Name LIKE N'Bicycle%'";

      const string key = nameof(Select052);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var store = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Store"]);
        var select = SqlDml.Select(store);
        select.Columns.AddRange(store["CustomerID"], store["Name"]);
        select.Where = SqlDml.Like(store["CustomerID"], "1%") || SqlDml.Like(store["Name"], "Bicycle%");
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select053()
    {
      //var nativeSql = "SELECT ProductID, ProductModelID "
      //  + "FROM AdventureWorks.Production.Product "
      //    + "WHERE ProductModelID = 20 OR ProductModelID = 21 "
      //      + "AND Color = 'Red'";

      const string key = nameof(Select053);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.AddRange(product["ProductID"], product["ProductModelID"]);
        select.Where = product["ProductModelID"] == 20 ||
          (product["ProductModelID"] == 21 && product["Color"] == "RED");
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select054()
    {
      //var nativeSql = "SELECT ProductID, ProductModelID "
      //  + "FROM AdventureWorks.Production.Product "
      //    + "WHERE (ProductModelID = 20 OR ProductModelID = 21) "
      //      + "AND Color = 'Red'";

      const string key = nameof(Select054);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.AddRange(product["ProductID"], product["ProductModelID"]);
        select.Where = (product["ProductModelID"] == 20 || product["ProductModelID"] == 21) &&
          product["Color"] == "RED";

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select055()
    {
      var nativeSql = "SELECT ProductID, ProductModelID "
        + "FROM AdventureWorks.Production.Product "
          + "WHERE ProductModelID = 20 OR (ProductModelID = 21 "
            + "AND Color = 'Red')";

      const string key = nameof(Select055);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.AddRange(product["ProductID"], product["ProductModelID"]);
        select.Where = product["ProductModelID"] == 20 ||
          (product["ProductModelID"] == 21 && product["Color"] == "RED");
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select056()
    {
      //var nativeSql = "SELECT SalesOrderID, SUM(LineTotal) AS SubTotal "
      //  + "FROM Sales.SalesOrderDetail sod "
      //    + "GROUP BY SalesOrderID "
      //      + "ORDER BY SalesOrderID ;";

      const string key = nameof(Select056);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var sod = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderDetail"], "sod");
        var select = SqlDml.Select(sod);
        select.Columns.Add(sod["SalesOrderID"]);
        select.Columns.Add(SqlDml.Sum(sod["LineTotal"]), "SubTotal");
        select.GroupBy.Add(sod["SalesOrderID"]);
        select.OrderBy.Add(sod["SalesOrderID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select057()
    {
      //var nativeSql = "SELECT DATEPART(yy, HireDate) AS Year, "
      //  + "COUNT(*) AS NumberOfHires "
      //    + "FROM AdventureWorks.HumanResources.Employee "
      //      + "GROUP BY DATEPART(yy, HireDate)";

      const string key = nameof(Select057);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var employee = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["Employee"]);
        var select = SqlDml.Select(employee);
        select.Columns.Add(
          SqlDml.Extract(SqlDateTimePart.Year, employee["HireDate"]), "Year");
        select.Columns.Add(SqlDml.Count(), "NumberOfHires");
        select.GroupBy.Add(SqlDml.Extract(SqlDateTimePart.Year, employee["HireDate"]));
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select058()
    {
      //var nativeSql = ""
      //  +
      //  "SELECT ProductID, SpecialOfferID, AVG(UnitPrice) AS 'Average Price', "
      //    + "SUM(LineTotal) AS SubTotal "
      //      + "FROM Sales.SalesOrderDetail "
      //        + "GROUP BY ProductID, SpecialOfferID "
      //          + "ORDER BY ProductID";

      const string key = nameof(Select058);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var salesOrderDetail = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderDetail"]);
        var select = SqlDml.Select(salesOrderDetail);
        select.Columns.Add(salesOrderDetail["ProductID"]);
        select.Columns.Add(salesOrderDetail["SpecialOfferID"]);
        select.Columns.Add(SqlDml.Avg(salesOrderDetail["UnitPrice"]), "Average Price");
        select.Columns.Add(SqlDml.Sum(salesOrderDetail["LineTotal"]), "SubTotal");
        select.GroupBy.AddRange(salesOrderDetail["ProductID"], salesOrderDetail["SpecialOfferID"]);
        select.OrderBy.Add(salesOrderDetail["ProductID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select059()
    {
      //var nativeSql = ""
      //  +
      //  "SELECT ProductID, SpecialOfferID, AVG(UnitPrice) AS 'Average Price', "
      //    + "SUM(LineTotal) AS SubTotal "
      //      + "FROM Sales.SalesOrderDetail "
      //        + "GROUP BY ProductID, SpecialOfferID "
      //          + "ORDER BY ProductID";

      const string key = nameof(Select059);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var salesOrderDetail = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderDetail"]);
        var select = SqlDml.Select(salesOrderDetail);
        select.Columns.Add(salesOrderDetail["ProductID"]);
        select.Columns.Add(salesOrderDetail["SpecialOfferID"]);
        select.Columns.Add(SqlDml.Avg(salesOrderDetail["UnitPrice"]), "Average Price");
        select.Columns.Add(SqlDml.Sum(salesOrderDetail["LineTotal"]), "SubTotal");
        select.GroupBy.AddRange(salesOrderDetail["ProductID"], salesOrderDetail["SpecialOfferID"]);
        select.OrderBy.Add(salesOrderDetail["ProductID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select060()
    {
      //var nativeSql = "SELECT ProductModelID, AVG(ListPrice) AS 'Average List Price' "
      //  + "FROM Production.Product "
      //    + "WHERE ListPrice > $1000 "
      //      + "GROUP BY ProductModelID "
      //        + "ORDER BY ProductModelID ;";

      const string key = nameof(Select060);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.Add(product["ProductModelID"]);
        select.Columns.Add(SqlDml.Avg(product["ListPrice"]), "Average List Price");
        select.Where = product["ListPrice"] > 1000;
        select.GroupBy.Add(product["ProductModelID"]);
        select.OrderBy.Add(product["ProductModelID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select061()
    {
      //var nativeSql =
        //"SELECT ProductID, AVG(OrderQty) AS AverageQuantity, SUM(LineTotal) AS Total "
        //  + "FROM Sales.SalesOrderDetail "
        //    + "GROUP BY ProductID "
        //      + "HAVING SUM(LineTotal) > $1000000.00 "
        //        + "AND AVG(OrderQty) < 3 ;";

      const string key = nameof(Select061);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var salesOrderDetail = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderDetail"]);
        var select = SqlDml.Select(salesOrderDetail);
        select.Columns.Add(salesOrderDetail["ProductID"]);
        select.Columns.Add(SqlDml.Avg(salesOrderDetail["OrderQty"]), "AverageQuantity");
        select.Columns.Add(SqlDml.Sum(salesOrderDetail["LineTotal"]), "Total");
        select.Having = SqlDml.Sum(salesOrderDetail["LineTotal"]) > 1000000 &&
          SqlDml.Avg(salesOrderDetail["OrderQty"]) < 3;
        select.GroupBy.Add(salesOrderDetail["ProductID"]);

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select062()
    {
      //var nativeSql = "SELECT ProductID, Total = SUM(LineTotal) "
      //  + "FROM Sales.SalesOrderDetail "
      //    + "GROUP BY ProductID "
      //      + "HAVING SUM(LineTotal) > $2000000.00 ;";

      const string key = nameof(Select062);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var salesOrderDetail = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderDetail"]);
        var select = SqlDml.Select(salesOrderDetail);
        select.Columns.Add(salesOrderDetail["ProductID"]);
        select.Columns.Add(SqlDml.Sum(salesOrderDetail["LineTotal"]), "Total");
        select.Having = SqlDml.Sum(salesOrderDetail["LineTotal"]) > 2000000;
        select.GroupBy.Add(salesOrderDetail["ProductID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select063()
    {
      //var nativeSql = "SELECT ProductID, SUM(LineTotal) AS Total "
      //  + "FROM Sales.SalesOrderDetail "
      //    + "GROUP BY ProductID "
      //      + "HAVING COUNT(*) > 1500 ;";

      const string key = nameof(Select063);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var salesOrderDetail = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderDetail"]);
        var select = SqlDml.Select(salesOrderDetail);
        select.Columns.Add(salesOrderDetail["ProductID"]);
        select.Columns.Add(SqlDml.Sum(salesOrderDetail["LineTotal"]), "Total");
        select.Having = SqlDml.Count() > 1500;
        select.GroupBy.Add(salesOrderDetail["ProductID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select064()
    {
      //var nativeSql = "SELECT ProductID "
      //  + "FROM Sales.SalesOrderDetail "
      //    + "GROUP BY ProductID "
      //      + "HAVING AVG(OrderQty) > 5 "
      //        + "ORDER BY ProductID ;";

      const string key = nameof(Select064);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var salesOrderDetail = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderDetail"]);
        var select = SqlDml.Select(salesOrderDetail);
        select.Columns.Add(salesOrderDetail["ProductID"]);
        select.GroupBy.Add(salesOrderDetail["ProductID"]);
        select.Having = SqlDml.Avg(salesOrderDetail["OrderQty"]) > 5;
        select.OrderBy.Add(salesOrderDetail["ProductID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select065()
    {
      //var nativeSql = "SELECT pm.Name, AVG(ListPrice) AS 'Average List Price' "
      //  + "FROM Production.Product AS p "
      //    + "JOIN Production.ProductModel AS pm "
      //      + "ON p.ProductModelID = pm.ProductModelID "
      //        + "GROUP BY pm.Name "
      //          + "HAVING pm.Name LIKE 'Mountain%' "
      //            + "ORDER BY pm.Name ;";

      const string key = nameof(Select065);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var p = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"], "p");
        var pm = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["ProductModel"], "pm");
        var select = SqlDml.Select(p.InnerJoin(pm, p["ProductModelID"] == pm["ProductModelID"]));
        select.Columns.Add(pm["Name"]);
        select.Columns.Add(SqlDml.Avg(p["ListPrice"]), "Average List Price");
        select.GroupBy.Add(pm["Name"]);
        select.Having = SqlDml.Like(pm["Name"], "Mountain%");
        select.OrderBy.Add(pm["Name"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select066()
    {
      //var nativeSql = "SELECT ProductID, AVG(UnitPrice) AS 'Average Price' "
      //  + "FROM Sales.SalesOrderDetail "
      //    + "WHERE OrderQty > 10 "
      //      + "GROUP BY ProductID "
      //        + "ORDER BY ProductID ;";

      const string key = nameof(Select066);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var salesOrderDetail = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderDetail"]);
        var select = SqlDml.Select(salesOrderDetail);
        select.Columns.Add(salesOrderDetail["ProductID"]);
        select.Columns.Add(SqlDml.Avg(salesOrderDetail["UnitPrice"]), "Average Price");
        select.Where = salesOrderDetail["OrderQty"] > 10;
        select.GroupBy.Add(salesOrderDetail["ProductID"]);
        select.OrderBy.Add(salesOrderDetail["ProductID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select067()
    {
      //var nativeSql = "SELECT Color, AVG (ListPrice) AS 'average list price' "
      //  + "FROM Production.Product "
      //    + "WHERE Color IS NOT NULL "
      //      + "GROUP BY Color "
      //        + "ORDER BY Color";

      const string key = nameof(Select067);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.Add(product["Color"]);
        select.Columns.Add(SqlDml.Avg(product["ListPrice"]), "average list price");
        select.Where = SqlDml.IsNotNull(product["Color"]);
        select.GroupBy.Add(product["Color"]);
        select.OrderBy.Add(product["Color"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select068()
    {
      //var nativeSql = "SELECT ProductID, ProductSubcategoryID, ListPrice "
      //  + "FROM Production.Product "
      //    + "ORDER BY ProductSubcategoryID DESC, ListPrice";

      const string key = nameof(Select068);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.AddRange(
          product["ProductID"], product["ProductSubcategoryID"], product["ListPrice"]);
        select.OrderBy.Add(product["ProductSubcategoryID"], false);
        select.OrderBy.Add(product["ListPrice"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select069()
    {
      //var nativeSql = "SELECT Color, AVG (ListPrice) AS 'average list price' "
      //  + "FROM Production.Product "
      //    + "GROUP BY Color "
      //      + "ORDER BY 'average list price'";

      const string key = nameof(Select069);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var select = SqlDml.Select(product);
        select.Columns.Add(product["Color"]);
        select.Columns.Add(SqlDml.Avg(product["ListPrice"]), "average list price");
        select.GroupBy.Add(product["Color"]);
        select.OrderBy.Add(select.Columns["average list price"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select070()
    {
      //var nativeSql =
        //"SELECT Ord.SalesOrderID, Ord.OrderDate, "
        //  + "(SELECT MAX(OrdDet.UnitPrice) "
        //    + "FROM AdventureWorks.Sales.SalesOrderDetail AS OrdDet "
        //      + "WHERE Ord.SalesOrderID = OrdDet.SalesOrderID) AS MaxUnitPrice "
        //  + "FROM AdventureWorks.Sales.SalesOrderHeader AS Ord";

      const string key = nameof(Select070);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var ordDet = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderDetail"], "OrdDet");
        var ord = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderHeader"], "Ord");
        var subSelect = SqlDml.Select(ordDet);
        subSelect.Columns.Add(SqlDml.Max(ordDet["UnitPrice"]));
        subSelect.Where = ord["SalesOrderID"] == ordDet["SalesOrderID"];
        var select = SqlDml.Select(ord);
        select.Columns.AddRange(ord["SalesOrderID"], ord["OrderDate"]);
        select.Columns.Add(subSelect, "MaxUnitPrice");
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select071()
    {
      //var nativeSql = "SELECT Name "
      //  + "FROM AdventureWorks.Production.Product "
      //    + "WHERE ListPrice = "
      //      + "(SELECT ListPrice "
      //        + "FROM AdventureWorks.Production.Product "
      //          + "WHERE Name = 'Chainring Bolts' )";

      const string key = nameof(Select071);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product1 = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var product2 = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var subSelect = SqlDml.Select(product2);
        subSelect.Columns.Add(product2["ListPrice"]);
        subSelect.Where = product2["Name"] == "Chainring Bolts";
        var select = SqlDml.Select(product1);
        select.Columns.AddRange(product1["Name"]);
        select.Where = product1["ListPrice"] == subSelect;
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select072()
    {
      //var nativeSql = "SELECT Prd1. Name "
      //  + "FROM AdventureWorks.Production.Product AS Prd1 "
      //    + "JOIN AdventureWorks.Production.Product AS Prd2 "
      //      + "ON (Prd1.ListPrice = Prd2.ListPrice) "
      //        + "WHERE Prd2. Name = 'Chainring Bolts'";

      const string key = nameof(Select072);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var prd1 = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"], "Prd1");
        var prd2 = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"], "Prd2");
        var select = SqlDml.Select(prd1.InnerJoin(prd2, prd1["ListPrice"] == prd2["ListPrice"]));
        select.Columns.Add(prd1["Name"]);
        select.Where = prd2["Name"] == "Chainring Bolts";
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select073()
    {
      //var nativeSql = "SELECT Name "
      //  + "FROM Sales.Store "
      //    + "WHERE Sales.Store.CustomerID NOT IN "
      //      + "(SELECT Sales.Customer.CustomerID "
      //        + "FROM Sales.Customer "
      //          + "WHERE TerritoryID = 5)";

      const string key = nameof(Select073);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var store = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Store"]);
        var customer = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Customer"]);
        var subSelect = SqlDml.Select(customer);
        subSelect.Columns.Add(customer["CustomerID"]);
        subSelect.Where = customer["TerritoryID"] == 5;
        var select = SqlDml.Select(store);
        select.Columns.Add(store["Name"]);
        select.Where = SqlDml.NotIn(store["CustomerID"], subSelect);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select074()
    {
      //var nativeSql = "SELECT EmployeeID, ManagerID "
      //  + "FROM HumanResources.Employee "
      //    + "WHERE ManagerID IN "
      //      + "(SELECT ManagerID "
      //        + "FROM HumanResources.Employee "
      //          + "WHERE EmployeeID = 12)";

      const string key = nameof(Select074);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var employee1 = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["Employee"]);
        var employee2 = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["Employee"]);
        var subSelect = SqlDml.Select(employee2);
        subSelect.Columns.Add(employee2["ManagerID"]);
        subSelect.Where = employee2["EmployeeID"] == 12;
        var select = SqlDml.Select(employee1);
        select.Columns.AddRange(employee1["EmployeeID"], employee1["ManagerID"]);
        select.Where = SqlDml.In(employee1["ManagerID"], subSelect);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select075()
    {
      //var nativeSql = "SELECT e1.EmployeeID, e1.ManagerID "
      //  + "FROM HumanResources.Employee AS e1 "
      //    + "INNER JOIN HumanResources.Employee AS e2 "
      //      + "ON e1.ManagerID = e2.ManagerID "
      //        + "AND e2.EmployeeID = 12";

      const string key = nameof(Select075);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var e1 = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["Employee"], "e1");
        var e2 = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["Employee"], "e2");
        var select =
          SqlDml.Select(e1.InnerJoin(e2, e1["ManagerID"] == e2["ManagerID"] && e2["EmployeeID"] == 12));
        select.Columns.AddRange(e1["EmployeeID"], e1["ManagerID"]);

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select076()
    {
      //var nativeSql = "SELECT e1.EmployeeID, e1.ManagerID "
      //  + "FROM HumanResources.Employee AS e1 "
      //    + "WHERE e1.ManagerID IN "
      //      + "(SELECT e2.ManagerID "
      //        + "FROM HumanResources.Employee AS e2 "
      //          + "WHERE e2.EmployeeID = 12)";

      const string key = nameof(Select076);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var e1 = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["Employee"], "e1");
        var e2 = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["Employee"], "e2");
        var subSelect = SqlDml.Select(e2);
        subSelect.Columns.Add(e2["ManagerID"]);
        subSelect.Where = e2["EmployeeID"] == 12;
        var select = SqlDml.Select(e1);
        select.Columns.AddRange(e1["EmployeeID"], e1["ManagerID"]);
        select.Where = SqlDml.In(e1["ManagerID"], subSelect);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select077()
    {
      //var nativeSql = "SELECT Name "
      //  + "FROM Production.Product "
      //    + "WHERE ProductSubcategoryID IN "
      //      + "(SELECT ProductSubcategoryID "
      //        + "FROM Production.ProductSubcategory "
      //          + "WHERE Name = 'Wheels')";

      const string key = nameof(Select077);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var productSubcategory =
          SqlDml.TableRef(Catalog.Schemas["Production"].Tables["ProductSubcategory"]);
        var subSelect = SqlDml.Select(productSubcategory);
        subSelect.Columns.Add(productSubcategory["ProductSubcategoryID"]);
        subSelect.Where = productSubcategory["Name"] == "Wheels";
        var select = SqlDml.Select(product);
        select.Columns.Add(product["Name"]);
        select.Where = SqlDml.In(product["ProductSubcategoryID"], subSelect);

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select078()
    {
      //var nativeSql = "SELECT p.Name, s.Name "
      //  + "FROM Production.Product p "
      //    + "INNER JOIN Production.ProductSubcategory s "
      //      + "ON p.ProductSubcategoryID = s.ProductSubcategoryID "
      //        + "AND s.Name = 'Wheels'";

      const string key = nameof(Select078);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var p = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"], "p");
        var s = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["ProductSubcategory"], "s");
        var select = SqlDml.Select(
          p.InnerJoin(
            s, p["ProductSubcategoryID"] == s["ProductSubcategoryID"] && s["Name"] == "Wheels"));
        select.Columns.AddRange(p["Name"], s["Name"]);

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select079()
    {
      //var nativeSql = "SELECT Name "
      //  + "FROM Purchasing.Vendor "
      //    + "WHERE CreditRating = 1 "
      //      + "AND VendorID IN "
      //        + "(SELECT VendorID "
      //          + "FROM Purchasing.ProductVendor "
      //            + "WHERE MinOrderQty >= 20 "
      //              + "AND AverageLeadTime < 16)";

      const string key = nameof(Select079);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var vendor = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["Vendor"]);
        var productVendor = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["ProductVendor"]);
        var subSelect = SqlDml.Select(productVendor);
        subSelect.Columns.Add(productVendor["VendorID"]);
        subSelect.Where = productVendor["MinOrderQty"] >= 20 && productVendor["AverageLeadTime"] < 16;
        var select = SqlDml.Select(vendor);
        select.Columns.Add(vendor["Name"]);
        select.Where = vendor["CreditRating"] == 1 &&
          SqlDml.In(vendor["VendorID"], subSelect);

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select080()
    {
      //var nativeSql = "SELECT DISTINCT Name "
      //  + "FROM Purchasing.Vendor v "
      //    + "INNER JOIN Purchasing.ProductVendor p "
      //      + "ON v.VendorID = p.VendorID "
      //        + "WHERE CreditRating = 1 "
      //          + "AND MinOrderQty >= 20 "
      //            + "AND OnOrderQty IS NULL";

      const string key = nameof(Select080);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var v = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["Vendor"], "v");
        var p = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["ProductVendor"], "p");
        var select = SqlDml.Select(v.InnerJoin(p, v["VendorID"] == p["VendorID"]));
        select.Distinct = true;
        select.Columns.Add(v["Name"]);
        select.Where = v["CreditRating"] == 1 && p["MinOrderQty"] >= 20 && SqlDml.IsNull(p["OnOrderQty"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select081()
    {
      //var nativeSql = "SELECT Name "
      //  + "FROM Production.Product "
      //    + "WHERE ProductSubcategoryID NOT IN "
      //      + "(SELECT ProductSubcategoryID "
      //        + "FROM Production.Product "
      //          + "WHERE Name = 'Mountain Bikes' "
      //            + "OR Name = 'Road Bikes' "
      //              + "OR Name = 'Touring Bikes')";

      const string key = nameof(Select081);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var product2 = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var subSelect = SqlDml.Select(product2);
        subSelect.Columns.Add(product2["ProductSubcategoryID"]);
        subSelect.Where = product2["Name"] == "Mountain Bikes" || product2["Name"] == "Road Bikes" ||
          product2["Name"] == "Touring Bikes";
        var select = SqlDml.Select(product);
        select.Columns.Add(product["Name"]);
        select.Where = SqlDml.NotIn(product["ProductSubcategoryID"], subSelect);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select082()
    {
      var nativeSql = "SELECT CustomerID "
        + "FROM Sales.Customer "
          + "WHERE TerritoryID = "
            + "(SELECT TerritoryID "
              + "FROM Sales.SalesPerson "
                + "WHERE SalesPersonID = 276)";

      const string key = nameof(Select082);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var customer = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Customer"]);
        var salesPerson = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesPerson"]);
        var subSelect = SqlDml.Select(salesPerson);
        subSelect.Columns.Add(salesPerson["TerritoryID"]);
        subSelect.Where = salesPerson["SalesPersonID"] == 276;
        var select = SqlDml.Select(customer);
        select.Columns.Add(customer["CustomerID"]);
        select.Where = customer["TerritoryID"] == subSelect;
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select083()
    {
      //var nativeSql = "SELECT Name "
      //  + "FROM Production.Product "
      //    + "WHERE ListPrice > "
      //      + "(SELECT AVG (ListPrice) "
      //        + "FROM Production.Product)";

      const string key = nameof(Select083);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product1 = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var product2 = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var subSelect = SqlDml.Select(product2);
        subSelect.Columns.Add(SqlDml.Avg(product2["ListPrice"]));
        var select = SqlDml.Select(product1);
        select.Columns.Add(product1["Name"]);
        select.Where = product1["ListPrice"] > subSelect;
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select084()
    {
      //var nativeSql = "SELECT Name "
      //  + "FROM Production.Product "
      //    + "WHERE ListPrice > "
      //      + "(SELECT MIN (ListPrice) "
      //        + "FROM Production.Product "
      //          + "GROUP BY ProductSubcategoryID "
      //            + "HAVING ProductSubcategoryID = 14)";

      const string key = nameof(Select084);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product1 = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var product2 = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var subSelect = SqlDml.Select(product2);
        subSelect.Columns.Add(SqlDml.Min(product2["ListPrice"]));
        subSelect.GroupBy.Add(product2["ProductSubcategoryID"]);
        subSelect.Having = product2["ProductSubcategoryID"] == 14;
        var select = SqlDml.Select(product1);
        select.Columns.Add(product1["Name"]);
        select.Where = product1["ListPrice"] > subSelect;
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select085()
    {
      //var nativeSql = "SELECT Name "
      //  + "FROM Production.Product "
      //    + "WHERE ListPrice >= ANY "
      //      + "(SELECT MAX (ListPrice) "
      //        + "FROM Production.Product "
      //          + "GROUP BY ProductSubcategoryID)";

      const string key = nameof(Select085);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product1 = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var product2 = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var subSelect = SqlDml.Select(product2);
        subSelect.Columns.Add(SqlDml.Max(product2["ListPrice"]));
        subSelect.GroupBy.Add(product2["ProductSubcategoryID"]);
        var select = SqlDml.Select(product1);
        select.Columns.Add(product1["Name"]);
        select.Where = product1["ListPrice"] >= SqlDml.Any(subSelect);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select086()
    {
      //var nativeSql = "SELECT Name "
      //  + "FROM Production.Product "
      //    + "WHERE ProductSubcategoryID=ANY "
      //      + "(SELECT ProductSubcategoryID "
      //        + "FROM Production.ProductSubcategory "
      //          + "WHERE Name = 'Wheels')";

      const string key = nameof(Select086);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var productSubcategory =
          SqlDml.TableRef(Catalog.Schemas["Production"].Tables["ProductSubcategory"]);
        var subSelect = SqlDml.Select(productSubcategory);
        subSelect.Columns.Add(productSubcategory["ProductSubcategoryID"]);
        subSelect.Where = productSubcategory["Name"] == "Wheels";
        var select = SqlDml.Select(product);
        select.Columns.Add(product["Name"]);
        select.Where = product["ProductSubcategoryID"] == SqlDml.Any(subSelect);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select087()
    {
      //var nativeSql = "SELECT Name "
      //  + "FROM Production.Product "
      //    + "WHERE ProductSubcategoryID IN "
      //      + "(SELECT ProductSubcategoryID "
      //        + "FROM Production.ProductSubcategory "
      //          + "WHERE Name = 'Wheels')";

      const string key = nameof(Select087);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var productSubcategory =
          SqlDml.TableRef(Catalog.Schemas["Production"].Tables["ProductSubcategory"]);
        var subSelect = SqlDml.Select(productSubcategory);
        subSelect.Columns.Add(productSubcategory["ProductSubcategoryID"]);
        subSelect.Where = productSubcategory["Name"] == "Wheels";
        var select = SqlDml.Select(product);
        select.Columns.Add(product["Name"]);
        select.Where = SqlDml.In(product["ProductSubcategoryID"], subSelect);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select088()
    {
      //var nativeSql = "SELECT CustomerID "
      //  + "FROM Sales.Customer "
      //    + "WHERE TerritoryID <> ANY "
      //      + "(SELECT TerritoryID "
      //        + "FROM Sales.SalesPerson)";

      const string key = nameof(Select088);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var customer = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Customer"]);
        var salesPerson = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesPerson"]);
        var subSelect = SqlDml.Select(salesPerson);
        subSelect.Columns.Add(salesPerson["TerritoryID"]);
        var select = SqlDml.Select(customer);
        select.Columns.Add(customer["CustomerID"]);
        select.Where = customer["TerritoryID"] != SqlDml.Any(subSelect);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select089()
    {
      //var nativeSql = "SELECT Name "
      //  + "FROM Production.Product "
      //    + "WHERE EXISTS "
      //      + "(SELECT * "
      //        + "FROM Production.ProductSubcategory "
      //          + "WHERE ProductSubcategoryID = "
      //            + "Production.Product.ProductSubcategoryID "
      //              + "AND Name = 'Wheels')";

      const string key = nameof(Select089);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var productSubcategory =
          SqlDml.TableRef(Catalog.Schemas["Production"].Tables["ProductSubcategory"]);
        var subSelect = SqlDml.Select(productSubcategory);
        subSelect.Columns.Add(productSubcategory.Asterisk);
        subSelect.Where = productSubcategory["ProductSubcategoryID"] == product["ProductSubcategoryID"];
        subSelect.Where = subSelect.Where && productSubcategory["Name"] == "Wheels";
        var select = SqlDml.Select(product);
        select.Columns.Add(product["Name"]);
        select.Where = SqlDml.Exists(subSelect);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select090()
    {
      //var nativeSql = "SELECT Name "
      //  + "FROM Production.Product "
      //    + "WHERE ProductSubcategoryID IN "
      //      + "(SELECT ProductSubcategoryID "
      //        + "FROM Production.ProductSubcategory "
      //          + "WHERE Name = 'Wheels')";

      const string key = nameof(Select090);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var productSubcategory =
          SqlDml.TableRef(Catalog.Schemas["Production"].Tables["ProductSubcategory"]);
        var subSelect = SqlDml.Select(productSubcategory);
        subSelect.Columns.Add(productSubcategory["ProductSubcategoryID"]);
        subSelect.Where = productSubcategory["Name"] == "Wheels";
        var select = SqlDml.Select(product);
        select.Columns.Add(product["Name"]);
        select.Where = SqlDml.In(product["ProductSubcategoryID"], subSelect);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select091()
    {
      //var nativeSql = "SELECT Name "
      //  + "FROM Production.Product "
      //    + "WHERE NOT EXISTS "
      //      + "(SELECT * "
      //        + "FROM Production.ProductSubcategory "
      //          + "WHERE ProductSubcategoryID = "
      //            + "Production.Product.ProductSubcategoryID "
      //              + "AND Name = 'Wheels')";

      const string key = nameof(Select091);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var productSubcategory =
          SqlDml.TableRef(Catalog.Schemas["Production"].Tables["ProductSubcategory"]);
        var subSelect = SqlDml.Select(productSubcategory);
        subSelect.Columns.Add(productSubcategory.Asterisk);
        subSelect.Where = productSubcategory["ProductSubcategoryID"] == product["ProductSubcategoryID"];
        subSelect.Where = subSelect.Where && productSubcategory["Name"] == "Wheels";
        var select = SqlDml.Select(product);
        select.Columns.Add(product["Name"]);
        select.Where = !SqlDml.Exists(subSelect);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select092()
    {
      //var nativeSql = "SELECT Name, ListPrice, "
      //  + "(SELECT AVG(ListPrice) FROM Production.Product) AS Average, "
      //    +
      //    "    ListPrice - (SELECT AVG(ListPrice) FROM Production.Product) "
      //      + "AS Difference "
      //        + "FROM Production.Product "
      //          + "WHERE ProductSubcategoryID = 1";

      const string key = nameof(Select092);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product1 = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var product2 = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var subSelect = SqlDml.Select(product2);
        subSelect.Columns.Add(SqlDml.Avg(product2["ListPrice"]));
        var select = SqlDml.Select(product1);
        select.Columns.AddRange(product1["Name"], product1["ListPrice"]);
        select.Columns.Add(subSelect, "Average");
        select.Columns.Add(product1["ListPrice"] - subSelect, "Difference");
        select.Where = product1["ProductSubcategoryID"] == 1;
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select093()
    {
      //var nativeSql = "SELECT LastName, FirstName "
      //  + "FROM Person.Contact "
      //    + "WHERE ContactID IN "
      //      + "(SELECT ContactID "
      //        + "FROM HumanResources.Employee "
      //          + "WHERE EmployeeID IN "
      //            + "(SELECT SalesPersonID "
      //              + "FROM Sales.SalesPerson))";

      const string key = nameof(Select093);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var contact = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["Contact"]);
        var employee = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["Employee"]);
        var salesPerson = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesPerson"]);
        var subSelect2 = SqlDml.Select(salesPerson);
        subSelect2.Columns.Add(salesPerson["SalesPersonID"]);
        var subSelect1 = SqlDml.Select(employee);
        subSelect1.Columns.Add(employee["ContactID"]);
        subSelect1.Where = SqlDml.In(employee["EmployeeID"], subSelect2);
        var select = SqlDml.Select(contact);
        select.Columns.AddRange(contact["LastName"], contact["FirstName"]);
        select.Where = SqlDml.In(contact["ContactID"], subSelect1);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select094()
    {
      //var nativeSql = "SELECT LastName, FirstName "
      //  + "FROM Person.Contact c "
      //    + "INNER JOIN HumanResources.Employee e "
      //      + "ON c.ContactID = e.ContactID "
      //        + "JOIN Sales.SalesPerson s "
      //          + "ON e.EmployeeID = s.SalesPersonID";

      const string key = nameof(Select094);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var c = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["Contact"], "c");
        var e = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["Employee"], "e");
        var s = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesPerson"], "s");
        var select = SqlDml.Select(c);
        select.From = select.From.InnerJoin(e, c["ContactID"] == e["ContactID"]);
        select.From = select.From.InnerJoin(s, e["EmployeeID"] == s["SalesPersonID"]);
        select.Columns.AddRange(c["LastName"], c["FirstName"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select095()
    {
      //var nativeSql = "SELECT DISTINCT c.LastName, c.FirstName "
      //  + "FROM Person.Contact c JOIN HumanResources.Employee e "
      //    + "ON e.ContactID = c.ContactID "
      //      + "WHERE 5000.00 IN "
      //        + "(SELECT Bonus "
      //          + "FROM Sales.SalesPerson sp "
      //            + "WHERE e.EmployeeID = sp.SalesPersonID);";

      const string key = nameof(Select095);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var c = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["Contact"], "c");
        var e = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["Employee"], "e");
        var sp = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesPerson"], "sp");
        var subSelect = SqlDml.Select(sp);
        subSelect.Columns.Add(sp["Bonus"]);
        subSelect.Where = e["EmployeeID"] == sp["SalesPersonID"];
        var select = SqlDml.Select(c.InnerJoin(e, c["ContactID"] == e["ContactID"]));
        select.Distinct = true;
        select.Columns.AddRange(c["LastName"], c["FirstName"]);
        select.Where = SqlDml.In(5000.00, subSelect);

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select096()
    {
      //var nativeSql = "SELECT LastName, FirstName "
      //  + "FROM Person.Contact c JOIN HumanResources.Employee e "
      //    + "ON e.ContactID = c.ContactID "
      //      + "WHERE 5000 IN (5000)";

      const string key = nameof(Select096);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var c = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["Contact"], "c");
        var e = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["Employee"], "e");
        var select = SqlDml.Select(c.InnerJoin(e, c["ContactID"] == e["ContactID"]));
        select.Columns.AddRange(c["LastName"], c["FirstName"]);
        select.Where = SqlDml.In(5000, SqlDml.Row(5000));
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select097()
    {
      //var nativeSql = "SELECT DISTINCT pv1.ProductID, pv1.VendorID "
      //  + "FROM Purchasing.ProductVendor pv1 "
      //    + "WHERE ProductID IN "
      //      + "(SELECT pv2.ProductID "
      //        + "FROM Purchasing.ProductVendor pv2 "
      //          + "WHERE pv1.VendorID <> pv2.VendorID) "
      //            + "ORDER  BY pv1.VendorID";

      const string key = nameof(Select097);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var pv1 = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["ProductVendor"], "pv1");
        var pv2 = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["ProductVendor"], "pv2");
        var subSelect = SqlDml.Select(pv2);
        subSelect.Columns.Add(pv2["ProductID"]);
        subSelect.Where = pv1["VendorID"] != pv2["VendorID"];
        var select = SqlDml.Select(pv1);
        select.Distinct = true;
        select.Columns.AddRange(pv1["ProductID"], pv1["VendorID"]);
        select.Where = SqlDml.In(pv1["ProductID"], subSelect);
        select.OrderBy.Add(pv1["VendorID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select098()
    {
      //var nativeSql = "SELECT DISTINCT pv1.ProductID, pv1.VendorID "
      //  + "FROM Purchasing.ProductVendor pv1 "
      //    + "INNER JOIN Purchasing.ProductVendor pv2 "
      //      + "ON pv1.ProductID = pv2.ProductID "
      //        + "AND pv1.VendorID <> pv2.VendorID "
      //          + "ORDER BY pv1.VendorID";

      const string key = nameof(Select098);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var pv1 = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["ProductVendor"], "pv1");
        var pv2 = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["ProductVendor"], "pv2");
        var select = SqlDml.Select(
          pv1.InnerJoin(pv2, pv1["ProductID"] == pv2["ProductID"] && pv1["VendorID"] != pv2["VendorID"]));
        select.Distinct = true;
        select.Columns.AddRange(pv1["ProductID"], pv1["VendorID"]);
        select.OrderBy.Add(pv1["VendorID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select099()
    {
      //var nativeSql = "SELECT ProductID, OrderQty "
      //  + "FROM Sales.SalesOrderDetail s1 "
      //    + "WHERE s1.OrderQty < "
      //      + "(SELECT AVG (s2.OrderQty) "
      //        + "FROM Sales.SalesOrderDetail s2 "
      //          + "WHERE s2.ProductID = s1.ProductID)";

      const string key = nameof(Select099);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var s1 = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderDetail"], "s1");
        var s2 = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderDetail"], "s2");
        var subSelect = SqlDml.Select(s2);
        subSelect.Columns.Add(SqlDml.Avg(s2["OrderQty"]));
        subSelect.Where = s2["ProductID"] == s1["ProductID"];
        var select = SqlDml.Select(s1);
        select.Columns.AddRange(s1["ProductID"], s1["OrderQty"]);
        select.Where = s1["OrderQty"] < subSelect;
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select100()
    {
      //var nativeSql = "SELECT p1.ProductSubcategoryID, p1.Name "
      //  + "FROM Production.Product p1 "
      //    + "WHERE p1.ListPrice > "
      //      + "(SELECT AVG (p2.ListPrice) "
      //        + "FROM Production.Product p2 "
      //          + "WHERE p1.ProductSubcategoryID = p2.ProductSubcategoryID)";

      const string key = nameof(Select100);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var p1 = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"], "p1");
        var p2 = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"], "p2");
        var subSelect = SqlDml.Select(p2);
        subSelect.Columns.Add(SqlDml.Avg(p2["ListPrice"]));
        subSelect.Where = p2["ProductSubcategoryID"] == p1["ProductSubcategoryID"];
        var select = SqlDml.Select(p1);
        select.Columns.AddRange(p1["ProductSubcategoryID"], p1["Name"]);
        select.Where = p1["ListPrice"] > subSelect;
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select101()
    {
      //var nativeSql = "SELECT p1.ProductModelID "
      //  + "FROM Production.Product p1 "
      //    + "GROUP BY p1.ProductModelID "
      //      + "HAVING MAX(p1.ListPrice) >= ALL "
      //        + "(SELECT 2 * AVG(p2.ListPrice) "
      //          + "FROM Production.Product p2 "
      //            + "WHERE p1.ProductModelID = p2.ProductModelID) ;";

      const string key = nameof(Select101);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var p1 = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"], "p1");
        var p2 = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"], "p2");
        var subSelect = SqlDml.Select(p2);
        subSelect.Columns.Add(2 * SqlDml.Avg(p2["ListPrice"]));
        subSelect.Where = p2["ProductModelID"] == p1["ProductModelID"];
        var select = SqlDml.Select(p1);
        select.Columns.Add(p1["ProductModelID"]);
        select.GroupBy.Add(p1["ProductModelID"]);
        select.Having = SqlDml.Max(p1["ListPrice"]) >= SqlDml.All(subSelect);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select102()
    {
      //var nativeSql = "SELECT ProductID, Purchasing.Vendor.VendorID, Name "
      //  + "FROM Purchasing.ProductVendor JOIN Purchasing.Vendor "
      //    + "ON (Purchasing.ProductVendor.VendorID = Purchasing.Vendor.VendorID) "
      //      + "WHERE StandardPrice > $10 "
      //        + "AND Name LIKE N'F%'";

      const string key = nameof(Select102);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var productVendor = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["ProductVendor"]);
        var vendor = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["Vendor"]);
        var select =
          SqlDml.Select(productVendor.InnerJoin(vendor, productVendor["VendorID"] == vendor["VendorID"]));
        select.Columns.AddRange(productVendor["ProductID"], vendor["VendorID"], vendor["Name"]);
        select.Where = productVendor["StandardPrice"] > 10 && SqlDml.Like(vendor["Name"], "F%");
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select103()
    {
      //var nativeSql = "SELECT pv.ProductID, v.VendorID, v.Name "
      //  + "FROM Purchasing.ProductVendor pv JOIN Purchasing.Vendor v "
      //    + "ON (pv.VendorID = v.VendorID) "
      //      + "WHERE StandardPrice > $10 "
      //        + "AND Name LIKE N'F%'";

      const string key = nameof(Select103);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var pv = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["ProductVendor"], "pv");
        var v = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["Vendor"], "v");
        var select = SqlDml.Select(pv.InnerJoin(v, pv["VendorID"] == v["VendorID"]));
        select.Columns.AddRange(pv["ProductID"], v["VendorID"], v["Name"]);
        select.Where = pv["StandardPrice"] > 10 && SqlDml.Like(v["Name"], "F%");
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select104()
    {
      //var nativeSql = "SELECT pv.ProductID, v.VendorID, v.Name "
      //  + "FROM Purchasing.ProductVendor pv, Purchasing.Vendor v "
      //    + "WHERE pv.VendorID = v.VendorID "
      //      + "AND StandardPrice > $10 "
      //        + "AND Name LIKE N'F%'";

      const string key = nameof(Select104);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var pv = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["ProductVendor"], "pv");
        var v = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["Vendor"], "v");
        var select = SqlDml.Select(pv.CrossJoin(v));
        select.Columns.AddRange(pv["ProductID"], v["VendorID"], v["Name"]);
        select.Where = pv["VendorID"] == v["VendorID"] && pv["StandardPrice"] > 10 &&
          SqlDml.Like(v["Name"], "F%");
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select105()
    {
      //var nativeSql = "SELECT e.EmployeeID "
      //  + "FROM HumanResources.Employee AS e "
      //    + "INNER JOIN Sales.SalesPerson AS s "
      //      + "ON e.EmployeeID = s.SalesPersonID";

      const string key = nameof(Select105);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var e = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["Employee"], "e");
        var s = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesPerson"], "s");
        var select = SqlDml.Select(e.InnerJoin(s, e["EmployeeID"] == s["SalesPersonID"]));
        select.Columns.Add(e["EmployeeID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select106()
    {
      //var nativeSql = "SELECT * "
      //  + "FROM HumanResources.Employee AS e "
      //    + "INNER JOIN Person.Contact AS c "
      //      + "ON e.ContactID = c.ContactID "
      //        + "ORDER BY c.LastName";

      const string key = nameof(Select106);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var e = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["Employee"], "e");
        var c = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["Contact"], "c");
        var select = SqlDml.Select(e.InnerJoin(c, e["ContactID"] == c["ContactID"]));
        select.Columns.Add(SqlDml.Asterisk);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select107()
    {
      //var nativeSql =
        //"SELECT DISTINCT p.ProductID, p.Name, p.ListPrice, sd.UnitPrice AS 'Selling Price' "
        //  + "FROM Sales.SalesOrderDetail AS sd "
        //    + "JOIN Production.Product AS p "
        //      + "ON sd.ProductID = p.ProductID AND sd.UnitPrice < p.ListPrice "
        //        + "WHERE p.ProductID = 718;";

      const string key = nameof(Select107);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var sd = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderDetail"], "sd");
        var p = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"], "p");
        var select = SqlDml.Select(
          sd.InnerJoin(p, sd["ProductID"] == p["ProductID"] && sd["UnitPrice"] < p["ListPrice"]));
        select.Distinct = true;
        select.Columns.AddRange(p["ProductID"], p["Name"], p["ListPrice"]);
        select.Columns.Add(sd["UnitPrice"], "Selling Price");
        select.Where = p["ProductID"] == 718;
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select108()
    {
      //var nativeSql = "SELECT DISTINCT p1.ProductSubcategoryID, p1.ListPrice "
      //  + "FROM Production.Product p1 "
      //    + "INNER JOIN Production.Product p2 "
      //      + "ON p1.ProductSubcategoryID = p2.ProductSubcategoryID "
      //        + "AND p1.ListPrice <> p2.ListPrice "
      //          + "WHERE p1.ListPrice < $15 AND p2.ListPrice < $15 "
      //            + "ORDER BY ProductSubcategoryID;";

      const string key = nameof(Select108);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var p1 = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"], "p1");
        var p2 = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"], "p2");
        var select = SqlDml.Select(
          p1.InnerJoin(
            p2,
            p1["ProductSubcategoryID"] == p2["ProductSubcategoryID"] &&
              p1["ListPrice"] != p2["ListPrice"]));
        select.Distinct = true;
        select.Columns.AddRange(p1["ProductSubcategoryID"], p1["ListPrice"]);
        select.Where = p1["ListPrice"] < 15 && p2["ListPrice"] < 15;
        select.OrderBy.Add(p1["ProductSubcategoryID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select109()
    {
      //var nativeSql = "SELECT DISTINCT p1.VendorID, p1.ProductID "
      //  + "FROM Purchasing.ProductVendor p1 "
      //    + "INNER JOIN Purchasing.ProductVendor p2 "
      //      + "ON p1.ProductID = p2.ProductID "
      //        + "WHERE p1.VendorID <> p2.VendorID "
      //          + "ORDER BY p1.VendorID";

      const string key = nameof(Select109);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var p1 = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["ProductVendor"], "p1");
        var p2 = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["ProductVendor"], "p2");
        var select = SqlDml.Select(p1.InnerJoin(p2, p1["ProductID"] == p2["ProductID"]));
        select.Distinct = true;
        select.Columns.AddRange(p1["VendorID"], p1["ProductID"]);
        select.Where = p1["VendorID"] != p2["VendorID"];
        select.OrderBy.Add(p1["VendorID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select110()
    {
      //var nativeSql = "SELECT p.Name, pr.ProductReviewID "
      //  + "FROM Production.Product p "
      //    + "LEFT OUTER JOIN Production.ProductReview pr "
      //      + "ON p.ProductID = pr.ProductID";

      const string key = nameof(Select110);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var p = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"], "p");
        var pr = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["ProductReview"], "pr");
        var select = SqlDml.Select(p.LeftOuterJoin(pr, p["ProductID"] == pr["ProductID"]));
        select.Columns.AddRange(p["Name"], pr["ProductReviewID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select111()
    {
      //var nativeSql = "SELECT st.Name AS Territory, sp.SalesPersonID "
      //  + "FROM Sales.SalesTerritory st "
      //    + "RIGHT OUTER JOIN Sales.SalesPerson sp "
      //      + "ON st.TerritoryID = sp.TerritoryID ;";

      const string key = nameof(Select111);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var st = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesTerritory"], "st");
        var sp = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesPerson"], "sp");
        var select = SqlDml.Select(st.RightOuterJoin(sp, st["TerritoryID"] == sp["TerritoryID"]));
        select.Columns.Add(st["Name"], "Territory");
        select.Columns.Add(sp["SalesPersonID"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select112()
    {
      //var nativeSql = "SELECT st.Name AS Territory, sp.SalesPersonID "
      //  + "FROM Sales.SalesTerritory st "
      //    + "RIGHT OUTER JOIN Sales.SalesPerson sp "
      //      + "ON st.TerritoryID = sp.TerritoryID "
      //        + "WHERE st.SalesYTD < $2000000;";

      const string key = nameof(Select112);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var st = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesTerritory"], "st");
        var sp = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesPerson"], "sp");
        var select = SqlDml.Select(st.RightOuterJoin(sp, st["TerritoryID"] == sp["TerritoryID"]));
        select.Columns.Add(st["Name"], "Territory");
        select.Columns.Add(sp["SalesPersonID"]);
        select.Where = st["SalesYTD"] < 2000000;
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select113()
    {
      //var nativeSql = "SELECT p.Name, sod.SalesOrderID "
      //  + "FROM Production.Product p "
      //    + "FULL OUTER JOIN Sales.SalesOrderDetail sod "
      //      + "ON p.ProductID = sod.ProductID "
      //        + "WHERE p.ProductID IS NULL "
      //          + "OR sod.ProductID IS NULL "
      //            + "ORDER BY p.Name ;";

      const string key = nameof(Select113);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var p = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"], "p");
        var sod = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderDetail"], "sod");
        var select = SqlDml.Select(p.FullOuterJoin(sod, p["ProductID"] == sod["ProductID"]));
        select.Columns.AddRange(p["Name"], sod["SalesOrderID"]);
        select.Where = SqlDml.IsNull(p["ProductID"]) || SqlDml.IsNull(sod["ProductID"]);
        select.OrderBy.Add(p["Name"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select114()
    {
      //var nativeSql = "SELECT e.EmployeeID, d.Name AS Department "
      //  + "FROM HumanResources.Employee e "
      //    + "CROSS JOIN HumanResources.Department d "
      //      + "ORDER BY e.EmployeeID, d.Name ;";

      const string key = nameof(Select114);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var e = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["Employee"], "e");
        var d = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["Department"], "d");
        var select = SqlDml.Select(e.CrossJoin(d));
        select.Columns.Add(e["EmployeeID"]);
        select.Columns.Add(d["Name"], "Department");
        select.OrderBy.Add(e["EmployeeID"]);
        select.OrderBy.Add(d["Name"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select115()
    {
      //var nativeSql = "SELECT DISTINCT pv1.ProductID, pv1.VendorID "
      //  + "FROM Purchasing.ProductVendor pv1 "
      //    + "INNER JOIN Purchasing.ProductVendor pv2 "
      //      + "ON pv1.ProductID = pv2.ProductID "
      //        + "AND pv1.VendorID <> pv2.VendorID "
      //          + "ORDER BY pv1.ProductID";

      const string key = nameof(Select115);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var pv1 = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["ProductVendor"], "pv1");
        var pv2 = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["ProductVendor"], "pv2");
        var select = SqlDml.Select(
          pv1.InnerJoin(pv2, pv1["ProductID"] == pv2["ProductID"] && pv1["VendorID"] != pv2["VendorID"]));
        select.Distinct = true;
        select.Columns.AddRange(pv1["ProductID"], pv1["VendorID"]);
        select.OrderBy.Add(pv1["ProductID"]);

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select116()
    {
      //var nativeSql = "SELECT p.Name, v.Name "
      //  + "FROM Production.Product p "
      //    + "JOIN Purchasing.ProductVendor pv "
      //      + "ON p.ProductID = pv.ProductID "
      //        + "JOIN Purchasing.Vendor v "
      //          + "ON pv.VendorID = v.VendorID "
      //            + "WHERE ProductSubcategoryID = 15 "
      //              + "ORDER BY v.Name";

      const string key = nameof(Select116);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var p = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"], "p");
        var pv = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["ProductVendor"], "pv");
        var v = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["Vendor"], "v");
        var select = SqlDml.Select(
          p.InnerJoin(pv, p["ProductID"] == pv["ProductID"]).InnerJoin(
            v, pv["VendorID"] == v["VendorID"]));
        select.Columns.AddRange(p["Name"], v["Name"]);
        select.Where = p["ProductSubcategoryID"] == 15;
        select.OrderBy.Add(v["Name"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select117()
    {
      //var nativeSql = "SELECT Name, "
      //  + "CASE Name "
      //    + "WHEN 'Human Resources' THEN 'HR' "
      //      + "WHEN 'Finance' THEN 'FI' "
      //        + "WHEN 'Information Services' THEN 'IS' "
      //          + "WHEN 'Executive' THEN 'EX' "
      //            + "WHEN 'Facilities and Maintenance' THEN 'FM' "
      //              + "END AS Abbreviation "
      //                + "FROM AdventureWorks.HumanResources.Department "
      //                  + "WHERE GroupName = 'Executive General and Administration'";

      const string key = nameof(Select117);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var department = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["Department"]);
        var c = SqlDml.Case(department["Name"]);
        _ = c.Add("Human Resources", "HR").Add("Finance", "FI").Add("Information Services", "IS").Add("Executive", "EX");
        c["Facilities and Maintenance"] = "FM";
        var select = SqlDml.Select(department);
        select.Columns.AddRange(department["Name"]);
        select.Columns.Add(c, "Abbreviation");
        select.Where = department["GroupName"] == "Executive General and Administration";
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select118()
    {
      //var nativeSql = "SELECT   ProductNumber, Category = "
      //  + "CASE ProductLine "
      //    + "WHEN 'R' THEN 'Road' "
      //      + "WHEN 'M' THEN 'Mountain' "
      //        + "WHEN 'T' THEN 'Touring' "
      //          + "WHEN 'S' THEN 'Other sale items' "
      //            + "ELSE 'Not for sale' "
      //              + "END, "
      //                + "Name "
      //                  + "FROM Production.Product "
      //                    + "ORDER BY ProductNumber;";

      const string key = nameof(Select118);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var c = SqlDml.Case(product["ProductLine"]);
        c["R"] = "Road";
        c["M"] = "Mountain";
        c["T"] = "Touring";
        c["S"] = "Other sale items";
        c.Else = "Not for sale";
        var select = SqlDml.Select(product);
        select.Columns.AddRange(product["ProductNumber"]);
        select.Columns.Add(c, "Category");
        select.Columns.Add(product["Name"]);
        select.OrderBy.Add(product["ProductNumber"]);
        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select119()
    {
      //var nativeSql = "SELECT   ProductNumber, Name, 'Price Range' = "
      //  + "CASE "
      //    + "WHEN ListPrice =  0 THEN 'Mfg item - not for resale' "
      //      + "WHEN ListPrice < 50 THEN 'Under $50' "
      //        + "WHEN ListPrice >= 50 and ListPrice < 250 THEN 'Under $250' "
      //          + "WHEN ListPrice >= 250 and ListPrice < 1000 THEN 'Under $1000' "
      //            + "ELSE 'Over $1000' "
      //              + "END "
      //                + "FROM Production.Product "
      //                  + "ORDER BY ProductNumber ;";

      const string key = nameof(Select119);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var c = SqlDml.Case();
        c[product["ListPrice"] == 0] = "Mfg item - not for resale";
        c[product["ListPrice"] < 50] = "Under $50";
        c[product["ListPrice"] >= 50 && product["ListPrice"] < 250] = "Under $250";
        c[product["ListPrice"] >= 250 && product["ListPrice"] < 1000] = "Under $1000";
        c.Else = "Over $1000";
        var select = SqlDml.Select(product);
        select.Columns.AddRange(product["ProductNumber"], product["Name"]);
        select.Columns.Add(c, "Price Range");
        select.OrderBy.Add(product["ProductNumber"]);

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select120()
    {
      //var nativeSql = "SELECT * "
      //  + "FROM Sales.Store s "
      //    + "WHERE s.Name IN ('West Side Mart', 'West Wind Distributors', 'Westside IsCyclic Store')";

      const string key = nameof(Select120);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var s = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Store"], "s");
        var select = SqlDml.Select(s);
        select.Columns.Add(SqlDml.Asterisk);
        select.Where =
          SqlDml.In(
            s["Name"], SqlDml.Array("West Side Mart", "West Wind Distributors", "Westside IsCyclic Store"));

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select121()
    {
      //var nativeSql = "SELECT * "
      //  + "FROM Sales.Store s "
      //    + "WHERE s.CustomerID IN (1, 2, 3)";

      const string key = nameof(Select121);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var s = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Store"], "s");
        var select = SqlDml.Select(s);
        select.Columns.Add(SqlDml.Asterisk);
        select.Where = SqlDml.In(s["CustomerID"], SqlDml.Array(1, 2, 3));

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select122()
    {
      //var nativeSql = "Select Top 10 * "
      //  + "From (Person.StateProvince a "
      //    + "inner hash join Person.CountryRegion b on a.StateProvinceCode=b.CountryRegionCode)"
      //      + "inner loop join "
      //        + "(Person.StateProvince c "
      //          + "inner merge join Person.CountryRegion d on c.StateProvinceCode=d.CountryRegionCode)"
      //            + " on a.CountryRegionCode=c.CountryRegionCode";

      const string key = nameof(Select122);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var a = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["StateProvince"], "a");
        var b = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["CountryRegion"], "b");
        var c = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["StateProvince"], "c");
        var d = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["CountryRegion"], "d");

        var ab = a.InnerJoin(b, a["StateProvinceCode"] == b["CountryRegionCode"]);
        var cd = c.InnerJoin(d, c["StateProvinceCode"] == d["CountryRegionCode"]);
        var abcd = SqlDml.Join(SqlJoinType.InnerJoin, ab, cd, a["CountryRegionCode"] == c["CountryRegionCode"]);

        var select = SqlDml.Select(abcd);
        select.Limit = 10;
        select.Columns.Add(SqlDml.Asterisk);
        select.Hints.Add(SqlDml.JoinHint(SqlJoinMethod.Hash, ab));
        select.Hints.Add(SqlDml.JoinHint(SqlJoinMethod.Merge, cd));
        select.Hints.Add(SqlDml.JoinHint(SqlJoinMethod.Loop, abcd));

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select123()
    {
      //var nativeSql = "Select Top 10 * "
      //  + "From (Person.StateProvince a "
      //    + "inner hash join Person.CountryRegion b on a.StateProvinceCode=b.CountryRegionCode)"
      //      + "inner loop join "
      //        + "(Person.StateProvince c "
      //          + "inner merge join Person.CountryRegion d on c.StateProvinceCode=d.CountryRegionCode)"
      //            + " on a.CountryRegionCode=c.CountryRegionCode";

      const string key = nameof(Select123);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var a = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["StateProvince"], "a");
        var b = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["CountryRegion"], "b");
        var c = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["StateProvince"], "c");
        var d = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["CountryRegion"], "d");

        var ab = a.InnerJoin(b, a["StateProvinceCode"] == b["CountryRegionCode"]);
        var cd = c.InnerJoin(d, c["StateProvinceCode"] == d["CountryRegionCode"]);
        var abcd = SqlDml.Join(SqlJoinType.InnerJoin, ab, cd, a["CountryRegionCode"] == c["CountryRegionCode"]);

        var select = SqlDml.Select(abcd);
        select.Limit = 10;
        select.Columns.Add(SqlDml.Asterisk);
        select.Hints.Add(SqlDml.JoinHint(SqlJoinMethod.Hash, b));
        select.Hints.Add(SqlDml.JoinHint(SqlJoinMethod.Merge, d));
        select.Hints.Add(SqlDml.JoinHint(SqlJoinMethod.Loop, abcd));

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select124()
    {
      //var nativeSql =
      //  "Select Top 10 EmailAddress " +
      //  "From Person.Contact a " +
      //  "Where EmailAddress Like 'a%'";

      const string key = nameof(Select124);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var c = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["Contact"]);
        var select = SqlDml.Select(c);
        select.Limit = 10;
        select.Columns.Add(c["EmailAddress"]);
        select.Where = SqlDml.Like(c["EmailAddress"], "a%");

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select125()
    {
      //var nativeSql =
      //  "Select Top 10 EmailAddress " +
      //  "From Person.Contact a " +
      //  "Where EmailAddress Like 'a%' " +
      //  "OPTION (FAST 10, KEEP PLAN, ROBUST PLAN)";

      const string key = nameof(Select125);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var c = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["Contact"]);
        var select = SqlDml.Select(c);
        select.Limit = 10;
        select.Columns.Add(c["EmailAddress"]);
        select.Where = SqlDml.Like(c["EmailAddress"], "a%");
        select.Hints.Add(SqlDml.FastFirstRowsHint(10));
        select.Hints.Add(SqlDml.NativeHint("KEEP PLAN, ROBUST PLAN"));

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select126()
    {
      //var nativeSql = "Select Top 10 EmailAddress "
      //  + "From Person.Contact a "
      //    + "Where EmailAddress Like 'a%' "
      //      + "UNION ALL "
      //        + "Select Top 10 EmailAddress "
      //          + "From Person.Contact b "
      //            + "Where EmailAddress Like 'b%' ";

      const string key = nameof(Select126);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var c = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["Contact"]);
        var select1 = SqlDml.Select(c);
        select1.Limit = 10;
        select1.Columns.Add(c["EmailAddress"]);
        select1.Where = SqlDml.Like(c["EmailAddress"], "a%");
        var select2 = SqlDml.Select(c);
        select2.Limit = 10;
        select2.Columns.Add(c["EmailAddress"]);
        select2.Where = SqlDml.Like(c["EmailAddress"], "a%");

        query = SqlDml.UnionAll(select1, select2);
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select127()
    {
      //var nativeSql = "SELECT a.f FROM ((SELECT 1 as f UNION SELECT 2) EXCEPT (SELECT 3 UNION SELECT 4)) a";

      const string key = nameof(Select127);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var s1 = SqlDml.Select();
        var s2 = SqlDml.Select();
        var s3 = SqlDml.Select();
        var s4 = SqlDml.Select();
        s1.Columns.Add(1, "f");
        s2.Columns.Add(2);
        s3.Columns.Add(3);
        s4.Columns.Add(4);
        var qr = SqlDml.QueryRef(s1.Union(s2).Except(s3.Union(s4)), "a");
        var select = SqlDml.Select(qr);
        select.Columns.Add(qr["f"]);

        query = select;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Select128()
    {
      //var nativeSql =
      //  "SELECT c.Name SubcategoryName, p.Name ProductName " +
      //    "FROM Production.ProductSubcategory c " +
      //      "CROSS APPLY (SELECT Name FROM Production.Product WHERE ProductSubcategoryID = c.ProductSubcategoryID) p " +
      //        "ORDER BY c.Name, p.Name";

      const string key = nameof(Select128);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var subcategories = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["ProductSubcategory"], "c");
        var products = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);

        var innerSelect = SqlDml.Select(products);
        innerSelect.Columns.Add(products.Columns["Name"]);
        innerSelect.Where = products.Columns["ProductSubcategoryID"] == subcategories.Columns["ProductSubcategoryID"];
        var innerQuery = SqlDml.QueryRef(innerSelect, "p");

        var categoryName = subcategories.Columns["Name"];
        var productName = innerQuery.Columns["Name"];

        var outerSelect = SqlDml.Select(subcategories.CrossApply(innerQuery));
        outerSelect.Columns.Add(categoryName, "SubcategoryName");
        outerSelect.Columns.Add(productName, "ProductName");
        outerSelect.OrderBy.Add(categoryName);
        outerSelect.OrderBy.Add(productName);

        query = outerSelect;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Insert01()
    {
      const string key = nameof(Insert01);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        //var nativeSql = "INSERT INTO Production.UnitMeasure "
        //+ "VALUES (N'F2', N'Square Feet', GETDATE());";

        var unitMeasure = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["UnitMeasure"]);
        var insert = SqlDml.Insert(unitMeasure);
        insert.Values[unitMeasure[0]] = "F2";
        insert.Values[unitMeasure[1]] = "Square Feet";
        insert.Values[unitMeasure[2]] = SqlDml.CurrentDate();
        query = insert;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Update01()
    {
      const string key = nameof(Update01);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        //var nativeSql = "UPDATE AdventureWorks.Production.Product "
        //+ "SET ListPrice = ListPrice * 1.1 "
        //  + "WHERE ProductModelID = 37;";

        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var update = SqlDml.Update(product);
        update.Values[product["ListPrice"]] = product["ListPrice"] * 1.1;
        update.Where = product["ProductModelID"] == 37;
        query = update;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Update02()
    {
      //var nativeSql = "UPDATE Person.Address "
      //  + "SET PostalCode = '98000' "
      //    + "WHERE City = 'Bothell';";

      const string key = nameof(Update02);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["Address"]);
        var update = SqlDml.Update(product);
        update.Values[product["PostalCode"]] = "98000";
        update.Where = product["City"] == "Bothell";
        query = update;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Update03()
    {
      //var nativeSql = "UPDATE Sales.SalesPerson "
      //  + "SET Bonus = 6000, CommissionPct = .10, SalesQuota = NULL;";

      const string key = nameof(Update03);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesPerson"]);
        var update = SqlDml.Update(product);
        update.Values[product["Bonus"]] = 6000;
        update.Values[product["CommissionPct"]] = .10;
        update.Values[product["SalesQuota"]] = SqlDml.Null;
        query = update;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Update04()
    {
      //var nativeSql = "UPDATE Production.Product "
      //  + "SET ListPrice = ListPrice * 2;";

      const string key = nameof(Update04);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var update = SqlDml.Update(product);
        update.Values[product["ListPrice"]] = product["ListPrice"] * 2;
        query = update;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Update05()
    {
      //var nativeSql = "UPDATE Sales.SalesPerson "
      //  + "SET SalesYTD = SalesYTD + "
      //    + "(SELECT SUM(so.SubTotal) "
      //      + "FROM Sales.SalesOrderHeader AS so "
      //        + "WHERE so.OrderDate = (SELECT MAX(OrderDate) "
      //          + "FROM Sales.SalesOrderHeader AS so2 "
      //            + "WHERE so2.SalesPersonID = "
      //              + "so.SalesPersonID) "
      //                + "AND Sales.SalesPerson.SalesPersonID = so.SalesPersonID "
      //                  + "GROUP BY so.SalesPersonID);";

      const string key = nameof(Update05);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var salesPerson = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesPerson"]);
        var so = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderHeader"], "so");
        var so2 = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderHeader"], "so2");
        var subSelect = SqlDml.Select(so2);
        subSelect.Columns.Add(SqlDml.Max(so2["OrderDate"]));
        subSelect.Where = so2["SalesPersonID"] == so["SalesPersonID"];
        var select = SqlDml.Select(so);
        select.Columns.Add(SqlDml.Sum(so["SubTotal"]));
        select.Where = so["OrderDate"] == subSelect && salesPerson["SalesPersonID"] == so["SalesPersonID"];
        select.GroupBy.Add(so["SalesPersonID"]);
        var update = SqlDml.Update(salesPerson);
        update.Values[salesPerson["SalesYTD"]] = salesPerson["SalesYTD"] + select;
        query = update;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Update06()
    {
      //var nativeSql = "UPDATE AdventureWorks.Sales.SalesReason "
      //  + "SET Name = N'Unknown' "
      //    + "WHERE Name = N'Other';";

      const string key = nameof(Update06);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var salesReason = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesReason"]);
        var update = SqlDml.Update(salesReason);
        update.Values[salesReason["Name"]] = "Unknown";
        update.Where = salesReason["Name"] == "Other";
        query = update;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Update07()
    {
      //var nativeSql = "UPDATE Sales.SalesPerson "
      //  + "SET SalesYTD = SalesYTD + SubTotal "
      //    + "FROM Sales.SalesPerson AS sp "
      //      + "JOIN Sales.SalesOrderHeader AS so "
      //        + "ON sp.SalesPersonID = so.SalesPersonID "
      //          + "AND so.OrderDate = (SELECT MAX(OrderDate) "
      //            + "FROM Sales.SalesOrderHeader "
      //              + "WHERE SalesPersonID = "
      //                + "sp.SalesPersonID);";

      const string key = nameof(Update07);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var salesPerson = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesPerson"]);
        var salesOrderHeader = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderHeader"]);
        var sp = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesPerson"], "sp");
        var so = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderHeader"], "so");
        var subSelect = SqlDml.Select(salesOrderHeader);
        subSelect.Columns.Add(SqlDml.Max(salesOrderHeader["OrderDate"]));
        subSelect.Where = salesOrderHeader["SalesPersonID"] == sp["SalesPersonID"];
        _ = SqlDml.Select(
          sp.InnerJoin(so, sp["SalesPersonID"] == so["SalesPersonID"] && so["OrderDate"] == subSelect));
        var update = SqlDml.Update(salesPerson);
        update.Values[salesPerson["SalesYTD"]] = salesPerson["SalesYTD"];
        query = update;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Update08()
    {
      //var nativeSql = "UPDATE Sales.SalesPerson "
      //  + "SET SalesYTD = SalesYTD + "
      //    + "(SELECT SUM(so.SubTotal) "
      //      + "FROM Sales.SalesOrderHeader AS so "
      //        + "WHERE so.OrderDate = (SELECT MAX(OrderDate) "
      //          + "FROM Sales.SalesOrderHeader AS so2 "
      //            + "WHERE so2.SalesPersonID = "
      //              + "so.SalesPersonID) "
      //                + "AND Sales.SalesPerson.SalesPersonID = so.SalesPersonID "
      //                  + "GROUP BY so.SalesPersonID);";

      const string key = nameof(Update08);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var salesPerson = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesPerson"]);
        var so2 = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderHeader"], "so2");
        var so = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesOrderHeader"], "so");
        var subSelect = SqlDml.Select(so2);
        subSelect.Columns.Add(SqlDml.Max(so2["OrderDate"]));
        subSelect.Where = so2["SalesPersonID"] == so["SalesPersonID"];
        var select = SqlDml.Select(so);
        select.Columns.Add(SqlDml.Sum(so["SubTotal"]));
        select.Where = so["OrderDate"] == subSelect && salesPerson["SalesPersonID"] == so["SalesPersonID"];
        select.GroupBy.Add(so["SalesPersonID"]);
        var update = SqlDml.Update(salesPerson);
        update.Values[salesPerson["SalesYTD"]] = salesPerson["SalesYTD"] + select;
        query = update;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Update09()
    {
      //var nativeSql = "UPDATE Sales.Store "
      //  + "SET SalesPersonID = 276 "
      //  + "WHERE SalesPersonID = 275;";

      const string key = nameof(Update09);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var store = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["Store"]);
        var update = SqlDml.Update(store);
        update.Values[store["SalesPersonID"]] = 276;
        update.Where = store["SalesPersonID"] == 275;
        query = update;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Update10()
    {
      //var nativeSql = "UPDATE HumanResources.Employee "
      //  + "SET VacationHours = VacationHours + 8 "
      //    + "FROM (SELECT TOP 10 EmployeeID FROM HumanResources.Employee "
      //      + "ORDER BY HireDate ASC) AS th "
      //        + "WHERE HumanResources.Employee.EmployeeID = th.EmployeeID;";

      const string key = nameof(Update10);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var employee = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["Employee"]);
        var employee2 = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["Employee"]);

        var select = SqlDml.Select(employee);
        select.Limit = 10;
        select.Columns.Add(employee["EmployeeID"]);
        select.OrderBy.Add(employee["HireDate"]);
        var th = SqlDml.QueryRef(select, "th");

        var update = SqlDml.Update(employee2);
        update.Values[employee2["VacationHours"]] = employee2["VacationHours"] + 8;
        update.From = th;
        update.Where = employee2["EmployeeID"] == th["EmployeeID"];
        query = update;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Update11()
    {
      //var nativeSql = "UPDATE Production.Product "
      //  + "SET ListPrice = ListPrice * 2 "
      //    + "WHERE ProductID IN "
      //      + "(SELECT ProductID "
      //        + "FROM Purchasing.ProductVendor "
      //          + "WHERE VendorID = 51);";

      const string key = nameof(Update11);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var productVendor = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["ProductVendor"]);
        var subSelect = SqlDml.Select(productVendor);
        subSelect.Columns.Add(productVendor["ProductID"]);
        subSelect.Where = productVendor["VendorID"] == 51;
        var update = SqlDml.Update(product);
        update.Values[product["ListPrice"]] = product["ListPrice"] * 2;
        update.Where = SqlDml.In(product["ProductID"], subSelect);
        query = update;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Update12()
    {
      //var nativeSql = "UPDATE Production.Product "
      //  + "SET ListPrice = ListPrice * 2 "
      //    + "FROM Production.Product AS p "
      //      + "INNER JOIN Purchasing.ProductVendor AS pv "
      //        + "ON p.ProductID = pv.ProductID AND pv.VendorID = 51;";

      const string key = nameof(Update12);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var p = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"], "p");
        var pv = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["ProductVendor"], "pv");

        var update = SqlDml.Update(product);
        update.Values[product["ListPrice"]] = p["ListPrice"] * 2;
        update.From = p.InnerJoin(pv, p["ProductID"] == pv["ProductID"] && pv["VendorID"] == 51);
        query = update;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Delete01()
    {
      //var nativeSql = "DELETE FROM Sales.SalesPersonQuotaHistory "
      //  + "WHERE SalesPersonID IN "
      //    + "(SELECT SalesPersonID "
      //      + "FROM Sales.SalesPerson "
      //        + "WHERE SalesYTD > 2500000.00);";

      const string key = nameof(Delete01);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var salesPersonQuotaHistory =
        SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesPersonQuotaHistory"]);
        var salesPerson = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesPerson"]);
        var subSelect = SqlDml.Select(salesPerson);
        subSelect.Columns.Add(salesPerson["SalesPersonID"]);
        subSelect.Where = salesPerson["SalesYTD"] > 2500000.00;
        var delete = SqlDml.Delete(salesPersonQuotaHistory);
        delete.Where = SqlDml.In(salesPersonQuotaHistory["SalesPersonID"], subSelect);
        query = delete;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Delete02()
    {
      //var nativeSql = "DELETE FROM Sales.SalesPersonQuotaHistory "
      //  + "WHERE SalesPersonID IN  "
      //    + "(SELECT SalesPersonID "
      //      + "FROM Sales.SalesPerson "
      //        + "WHERE SalesYTD > 2500000.00);";

      const string key = nameof(Delete02);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var salesPersonQuotaHistory =
        SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesPersonQuotaHistory"]);

        var sp = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SalesPerson"]);
        var subSelect = SqlDml.Select(sp);
        subSelect.Columns.Add(sp["SalesPersonID"]);
        subSelect.Where = sp["SalesYTD"] > 2500000.00;

        var delete = SqlDml.Delete(salesPersonQuotaHistory);
        delete.Where = SqlDml.In(salesPersonQuotaHistory["SalesPersonID"], subSelect);
        query = delete;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Delete03()
    {
      //var nativeSql = "DELETE FROM Purchasing.PurchaseOrderDetail "
      //  + "WHERE PurchaseOrderDetailID IN "
      //    + "(SELECT TOP 10 PurchaseOrderDetailID "
      //      + "FROM Purchasing.PurchaseOrderDetail "
      //        + "ORDER BY DueDate ASC);";

      const string key = nameof(Delete03);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var purchaseOrderDetail1 =
        SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["PurchaseOrderDetail"]);
        var purchaseOrderDetail2 =
          SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["PurchaseOrderDetail"]);
        var select = SqlDml.Select(purchaseOrderDetail2);
        select.Limit = 10;
        select.Columns.Add(purchaseOrderDetail2["PurchaseOrderDetailID"]);
        select.OrderBy.Add(purchaseOrderDetail2["DueDate"]);
        var delete = SqlDml.Delete(purchaseOrderDetail1);
        delete.Where = SqlDml.In(purchaseOrderDetail1["PurchaseOrderDetailID"], select);
        query = delete;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Delete04()
    {
      //var nativeSql =
      //  "DELETE FROM [Sales].[SpecialOfferProduct] WHERE NOT EXISTS (SELECT [ProductID] FROM [Production].[Product]"
      //+ " WHERE [Production].[Product].[ProductID] = [Sales].[SpecialOfferProduct].[ProductID])";

      const string key = nameof(Delete04);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var products = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var specialOfferProduct = SqlDml.TableRef(Catalog.Schemas["Sales"].Tables["SpecialOfferProduct"]);

        var select = SqlDml.Select(products);
        select.Columns.Add(products["ProductID"]);
        select.Where = products["ProductID"] == specialOfferProduct["ProductID"];

        var delete = SqlDml.Delete(specialOfferProduct);
        delete.Where = SqlDml.Not(SqlDml.Exists(select));
        query = delete;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Delete05()
    {
      //var nativeSql = "DELETE FROM Production.Product "
      //  + "FROM Production.Product AS p "
      //    + "INNER JOIN Purchasing.ProductVendor AS pv "
      //      + "ON p.ProductID = pv.ProductID AND pv.VendorID = 0;";

      const string key = nameof(Delete05);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var product = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"]);
        var p = SqlDml.TableRef(Catalog.Schemas["Production"].Tables["Product"], "p");
        var pv = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["ProductVendor"], "pv");

        var delete = SqlDml.Delete(product);
        delete.From = p.InnerJoin(pv, p["ProductID"] == pv["ProductID"] && pv["VendorID"] == 0);
        query = delete;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Variable01()
    {
      // var nativeSql = "DECLARE @EmpIDVar int; "
      //   +"SET @EmpIDVar = 1234; "
      //     +"SELECT * "
      //       +"FROM HumanResources.Employee "
      //         +"WHERE EmployeeID = @EmpIDVar;";

      const string key = nameof(Variable01);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var employee = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["Employee"]);
        var empIDVar = SqlDml.Variable("EmpIDVar", SqlType.Int32);
        var batch = SqlDml.Batch();
        batch.Add(empIDVar.Declare());
        batch.Add(SqlDml.Assign(empIDVar, 1234));
        var select = SqlDml.Select(employee);
        select.Columns.Add(employee.Asterisk);
        select.Where = employee["EmployeeID"] == empIDVar;
        batch.Add(select);
        query = batch;
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string Variable02()
    {
      //var nativeSql = "DECLARE @find varchar(30); "
      //  + "SET @find = 'Man%'; "
      //    + "SELECT LastName, FirstName, Phone "
      //      + "FROM Person.Contact "
      //        + "WHERE LastName LIKE @find;";

      const string key = nameof(Variable02);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var contact = SqlDml.TableRef(Catalog.Schemas["Person"].Tables["Contact"]);
        var find = SqlDml.Variable("find", new SqlValueType("varchar(30)"));
        var batch = SqlDml.Batch();
        batch.Add(find.Declare());
        batch.Add(SqlDml.Assign(find, "Man%"));
        var select = SqlDml.Select(contact);
        select.Columns.AddRange(contact["LastName"], contact["FirstName"], contact["Phone"]);
        select.Where = SqlDml.Like(contact["LastName"], find);
        batch.Add(select);

        query = batch;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Cursor01()
    {
      //var nativeSql = "DECLARE complex_cursor CURSOR FOR "
      //  + "SELECT a.EmployeeID "
      //    + "FROM HumanResources.EmployeePayHistory AS a "
      //      + "WHERE RateChangeDate <> "
      //        + "(SELECT MAX(RateChangeDate) "
      //          + "FROM HumanResources.EmployeePayHistory AS b "
      //            + "WHERE a.EmployeeID = b.EmployeeID); "
      //              + "OPEN complex_cursor; "
      //                + "FETCH FROM complex_cursor; "
      //                  + "UPDATE HumanResources.EmployeePayHistory "
      //                    + "SET PayFrequency = 2 "
      //                      + "WHERE CURRENT OF complex_cursor; "
      //                        + "CLOSE complex_cursor; "
      //                          + "DEALLOCATE complex_cursor;";

      const string key = nameof(Cursor01);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var batch = SqlDml.Batch();
        var employeePayHistory = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["EmployeePayHistory"]);
        var a = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["EmployeePayHistory"], "a");
        var b = SqlDml.TableRef(Catalog.Schemas["HumanResources"].Tables["EmployeePayHistory"], "b");
        var selectInner = SqlDml.Select(b);
        selectInner.Columns.Add(SqlDml.Max(b["RateChangeDate"]));
        selectInner.Where = a["EmployeeID"] == b["EmployeeID"];
        var select = SqlDml.Select(a);
        select.Columns.Add(a["EmployeeID"]);
        select.Where = a["RateChangeDate"] != selectInner;
        var cursor = SqlDml.Cursor("complex_cursor", select);
        batch.Add(cursor.Declare());
        batch.Add(cursor.Open());
        batch.Add(cursor.Fetch());
        var update = SqlDml.Update(employeePayHistory);
        update.Values[employeePayHistory["PayFrequency"]] = 2;
        update.Where = cursor;
        batch.Add(update);
        batch.Add(cursor.Close());

        query = batch;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Cursor02()
    {
      //var nativeSql =
      //  "DECLARE test202cursor CURSOR FOR SELECT * FROM Purchasing.Vendor;\n" +
      //  "OPEN test202cursor;\n" +
      //  "FETCH NEXT FROM test202cursor;\n" +
      //  "CLOSE test202cursor;";

      const string key = nameof(Cursor02);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var batch = SqlDml.Batch();
        var vendors = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["Vendor"]);
        var select = SqlDml.Select(vendors);
        select.Columns.Add(select.Asterisk);
        var cursor = SqlDml.Cursor("test202cursor_", select);
        batch.Add(cursor.Declare());
        batch.Add(cursor.Open());
        batch.Add(cursor.Fetch());
        batch.Add(cursor.Close());

        query = batch;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string Cursor03()
    {
      //var nativeSql =
      //  "DECLARE test205cursor CURSOR FOR SELECT * FROM Purchasing.Vendor;\n" +
      //  "OPEN test205cursor;\n" +
      //  "BEGIN\n" +
      //  "  FETCH NEXT FROM test205cursor;\n" +
      //  "END\n" +
      //  "CLOSE test205cursor;";

      const string key = nameof(Cursor03);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var batch = SqlDml.Batch();
        var vendors = SqlDml.TableRef(Catalog.Schemas["Purchasing"].Tables["Vendor"]);
        var select = SqlDml.Select(vendors);
        select.Columns.Add(select.Asterisk);
        var cursor = SqlDml.Cursor("test205cursor_", select);
        batch.Add(cursor.Declare());
        batch.Add(cursor.Open());
        var block = SqlDml.StatementBlock();
        block.Add(cursor.Fetch());
        batch.Add(block);
        batch.Add(cursor.Close());

        query = batch;
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string DropTable01()
    {
      const string key = nameof(DropTable01);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        query = SqlDdl.Drop(Catalog.Schemas["HumanResources"].Tables["EmployeePayHistory"]);
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string DropTable02()
    {
      const string key = nameof(DropTable02);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        query = SqlDdl.Drop(Catalog.Schemas["HumanResources"].Tables["EmployeePayHistory"], false);
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string DropSchema01()
    {
      const string key = nameof(DropSchema01);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        query = SqlDdl.Drop(Catalog.Schemas["HumanResources"]);
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string DropSchema02()
    {
      const string key = nameof(DropSchema02);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        query = SqlDdl.Drop(Catalog.Schemas["HumanResources"], false);
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string CreateTable01()
    {
      const string key = nameof(CreateTable01);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var create = SqlDdl.Create(Catalog.Schemas["Production"].Tables["Product"]);
        create.Table.Filegroup = "xxx";
        query = create;
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string CreateTable02()
    {
      const string key = nameof(CreateTable02);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        query = SqlDdl.Create(Catalog.Schemas["Purchasing"].Tables["PurchaseOrderDetail"]);
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string CreateTableWithPartitionSchema1()
    {
      const string key = nameof(CreateTableWithPartitionSchema1);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var ps = Catalog.CreatePartitionSchema("ps190", null, "[PRIMARY]");
        var t = Catalog.Schemas["Production"].Tables["Product"];
        t.PartitionDescriptor = new PartitionDescriptor(t, t.TableColumns["ProductID"], ps);
        query = SqlDdl.Create(t);
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string CreateTableWithPartitionDescriptor1()
    {
      const string key = nameof(CreateTableWithPartitionDescriptor1);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var t = Catalog.Schemas["Production"].Tables["Product"];
        t.PartitionDescriptor = new PartitionDescriptor(t, t.TableColumns["ProductID"], PartitionMethod.Hash, 10);
        query = SqlDdl.Create(t);
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string CreateTableWithPartitionDescriptor2()
    {
      const string key = nameof(CreateTableWithPartitionDescriptor2);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var t = Catalog.Schemas["Production"].Tables["Product"];
        t.PartitionDescriptor = new PartitionDescriptor(t, t.TableColumns["ProductID"], PartitionMethod.Hash);
        _ = t.PartitionDescriptor.CreateHashPartition("ts1");
        _ = t.PartitionDescriptor.CreateHashPartition("ts2");
        _ = t.PartitionDescriptor.CreateHashPartition("ts3");
        query = SqlDdl.Create(t);
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string CreateTableWithPartitionDescriptor3()
    {
      const string key = nameof(CreateTableWithPartitionDescriptor3);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var t = Catalog.Schemas["Production"].Tables["Product"];
        t.PartitionDescriptor = new PartitionDescriptor(t, t.TableColumns["ProductID"], PartitionMethod.List);
        _ = t.PartitionDescriptor.CreateListPartition("p1", "ts1", "sdf", "sdf1", "sdfg");
        _ = t.PartitionDescriptor.CreateListPartition("p2", "ts2", "sir");
        query = SqlDdl.Create(t);
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string CreateTableWithPartitionDescriptor4()
    {
      const string key = nameof(CreateTableWithPartitionDescriptor4);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var t = Catalog.Schemas["Production"].Tables["Product"];
        t.PartitionDescriptor = new PartitionDescriptor(t, t.TableColumns["ProductID"], PartitionMethod.Range);
        _ = t.PartitionDescriptor.CreateRangePartition("ts1", new DateTime(2006, 01, 01).ToString());
        _ = t.PartitionDescriptor.CreateRangePartition("ts2", new DateTime(2007, 01, 01).ToString());
        _ = t.PartitionDescriptor.CreateRangePartition("ts3", "MAXVALUE");
        query = SqlDdl.Create(t);
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string CreateAssertion01()
    {
      const string key = nameof(CreateAssertion01);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var assertion = Catalog.Schemas["Production"]
        .CreateAssertion("assertion", SqlDml.Literal(1) == 1, false, false);
        query = SqlDdl.Create(assertion);
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string CreateDomain01()
    {
      const string key = nameof(CreateDomain01);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var domain =
        Catalog.Schemas["HumanResources"].CreateDomain("domain", new SqlValueType(SqlType.Decimal, 8, 2), 1);
        domain.Collation = Catalog.Schemas["HumanResources"].Collations["SQL_Latin1_General_CP1_CI_AS"];
        _ = domain.CreateConstraint("domainConstraint", SqlDml.Literal(1) == 1);
        query = SqlDdl.Create(domain);
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string CreateSchema01()
    {
      const string key = nameof(CreateSchema01);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        query = SqlDdl.Create(Catalog.Schemas["Production"]);
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string AlterTable01()
    {
      const string key = nameof(AlterTable01);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var alter = SqlDdl.Alter(
        Catalog.Schemas["Production"].Tables["Product"],
        SqlDdl.AddColumn(Catalog.Schemas["Production"].Tables["Product"].TableColumns["Name"]));

        query = alter;
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string AlterTable02()
    {
      const string key = nameof(AlterTable02);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var alter = SqlDdl.Alter(
        Catalog.Schemas["Production"].Tables["Product"],
          SqlDdl.DropDefault(Catalog.Schemas["Production"].Tables["Product"].TableColumns["Name"]));

        query = alter;
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string AlterTable03()
    {
      const string key = nameof(AlterTable03);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var alter = SqlDdl.Alter(
        Catalog.Schemas["Production"].Tables["Product"],
          SqlDdl.SetDefault("Empty", Catalog.Schemas["Production"].Tables["Product"].TableColumns["Name"]));
        query = alter;
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string AlterTable04()
    {
      const string key = nameof(AlterTable04);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var alter = SqlDdl.Alter(
        Catalog.Schemas["Production"].Tables["Product"],
          SqlDdl.DropColumn(Catalog.Schemas["Production"].Tables["Product"].TableColumns["Name"]));

        query = alter;
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string AlterTable05()
    {
      const string key = nameof(AlterTable05);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var alter = SqlDdl.Alter(
        Catalog.Schemas["Production"].Tables["Product"],
          SqlDdl.AddConstraint(Catalog.Schemas["Production"].Tables["Product"].TableConstraints[0]));

        query = alter;
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string AlterTable06()
    {
      const string key = nameof(AlterTable06);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var alter = SqlDdl.Alter(
        Catalog.Schemas["Production"].Tables["Product"],
          SqlDdl.DropConstraint(Catalog.Schemas["Production"].Tables["Product"].TableConstraints[0]));

        query = alter;
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string CreateSequence01()
    {
      const string key = nameof(CreateSequence01);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var sequence = Catalog.Schemas["Production"].CreateSequence("Generator175");
        query = SqlDdl.Create(sequence);
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string CreateSequence02()
    {
      const string key = nameof(CreateSequence02);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var seq = Catalog.Schemas["Production"].CreateSequence("Generator176");
        seq.SequenceDescriptor.IsCyclic = true;
        seq.SequenceDescriptor.StartValue = 1000;
        seq.SequenceDescriptor.MaxValue = 1000;
        seq.SequenceDescriptor.MinValue = -1000;
        seq.SequenceDescriptor.Increment = -1;
        query = SqlDdl.Create(seq);
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string DropSequence01()
    {
      const string key = nameof(DropSequence01);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var sequence = Catalog.Schemas["Production"].CreateSequence("Generator177");
        query = SqlDdl.Drop(sequence);
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string CreatePartitionFunction()
    {
      const string key = nameof(CreatePartitionFunction);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var pf =
        Catalog.CreatePartitionFunction("pf182", new SqlValueType(SqlType.Decimal, 5, 2), "1", "5", "10");
        pf.BoundaryType = BoundaryType.Right;
        query = SqlDdl.Create(pf);
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string DropPartitionFunctuon()
    {
      const string key = nameof(DropPartitionFunctuon);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var pf =
        Catalog.CreatePartitionFunction("pf183", new SqlValueType(SqlType.Decimal, 5, 2), "1", "5", "10");
        query = SqlDdl.Drop(pf);
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string AlterPartitionFunction()
    {
      const string key = nameof(AlterPartitionFunction);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var pf =
          Catalog.CreatePartitionFunction("pf184", new SqlValueType(SqlType.Decimal, 5, 2), "1", "5", "10");
        query = SqlDdl.Alter(pf, "5", SqlAlterPartitionFunctionOption.Split);
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string CreatePartitionSchema01()
    {
      const string key = nameof(CreatePartitionSchema01);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var pf =
          Catalog.CreatePartitionFunction("pf185", new SqlValueType(SqlType.Decimal, 5, 2), "1", "5", "10");
        var ps = Catalog.CreatePartitionSchema("ps1", pf, "[PRIMARY]", "sdf", "sdf1", "sdf2");
        query = SqlDdl.Create(ps);
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string CreatePartitionSchema02()
    {
      const string key = nameof(CreatePartitionSchema02);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var pf =
          Catalog.CreatePartitionFunction("pf186", new SqlValueType(SqlType.Decimal, 5, 2), "1", "5", "10");
        var ps = Catalog.CreatePartitionSchema("ps186", pf, "[PRIMARY]");
        query = SqlDdl.Create(ps);
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string DropPartitionSchema()
    {
      const string key = nameof(DropPartitionSchema);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var ps = Catalog.CreatePartitionSchema("ps187", null, "[PRIMARY]");
        query = SqlDdl.Drop(ps);
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string AlterPartitionSchema01()
    {
      const string key = nameof(AlterPartitionSchema01);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var ps = Catalog.CreatePartitionSchema("ps188", null, "[PRIMARY]");
        query = SqlDdl.Alter(ps);
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string AlterPartitionSchema02()
    {
      const string key = nameof(AlterPartitionSchema02);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var ps = Catalog.CreatePartitionSchema("ps189", null, "[PRIMARY]");
        query = SqlDdl.Alter(ps, "sdfg");
        _ = queriesCache.TryAdd(key, query);
      }
      return Compile(query);
    }

    public string CreateIndex()
    {
      const string key = nameof(CreateIndex);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var t = Catalog.Schemas["Production"].Tables["Product"];
        var index = t.CreateIndex("MegaIndex195");
        _ = index.CreateIndexColumn(t.TableColumns[0]);
        _ = index.CreateIndexColumn(t.TableColumns[1]);
        _ = index.CreateIndexColumn(t.TableColumns[2], false);
        _ = index.CreateIndexColumn(t.TableColumns[3]);
        _ = index.CreateIndexColumn(t.TableColumns[4]);
        _ = index.CreateIndexColumn(t.TableColumns[5]);
        index.IsUnique = true;
        index.IsClustered = true;
        index.FillFactor = 80;
        index.Filegroup = "\"default\"";

        query = SqlDdl.Create(index);
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

    public string DropIndex()
    {
      const string key = nameof(DropIndex);

      if (!queriesCache.TryGetValue(key, out var query))
      {
        var t = Catalog.Schemas["Production"].Tables["Product"];
        var index = t.CreateIndex("MegaIndex196");
        query = SqlDdl.Drop(index);
        _ = queriesCache.TryAdd(key, query);
      }

      return Compile(query);
    }

#pragma warning restore IDE0059, CS0219 // Unnecessary assignment of a value


    private Catalog CreateCatalogInMemory()
    {
      var catalog = new Catalog("AdventureWorks");

      _ = catalog.CreateSchema("HumanResources");
      _ = catalog.CreateSchema("Person");
      _ = catalog.CreateSchema("Production");
      _ = catalog.CreateSchema("Purchasing");
      _ = catalog.CreateSchema("Sales");

      _ = catalog.Schemas["HumanResources"].CreateCollation("Traditional_Spanish_CI_AI");
      _ = catalog.Schemas["Person"].CreateCollation("Traditional_Spanish_CI_AI");
      _ = catalog.Schemas["Production"].CreateCollation("Traditional_Spanish_CI_AI");
      _ = catalog.Schemas["Purchasing"].CreateCollation("Traditional_Spanish_CI_AI");
      _ = catalog.Schemas["Sales"].CreateCollation("Traditional_Spanish_CI_AI");

      _ = catalog.Schemas["HumanResources"].CreateCollation("SQL_Latin1_General_CP1_CI_AS");
      _ = catalog.Schemas["Person"].CreateCollation("SQL_Latin1_General_CP1_CI_AS");
      _ = catalog.Schemas["Production"].CreateCollation("SQL_Latin1_General_CP1_CI_AS");
      _ = catalog.Schemas["Purchasing"].CreateCollation("SQL_Latin1_General_CP1_CI_AS");
      _ = catalog.Schemas["Sales"].CreateCollation("SQL_Latin1_General_CP1_CI_AS");

      Table t;
      TableColumn c;
      Constraint cs;

      t = catalog.Schemas["Production"].CreateTable("TransactionHistoryArchive");
      _ = t.CreateColumn("TransactionID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ProductID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ReferenceOrderID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ReferenceOrderLineID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("TransactionDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("TransactionType", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("Quantity", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ActualCost", new SqlValueType("money"));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("CreditCard");
      _ = t.CreateColumn("CreditCardID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("CardType", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("CardNumber", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ExpMonth", new SqlValueType(SqlType.UInt8));
      _ = t.CreateColumn("ExpYear", new SqlValueType(SqlType.Int16));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("Document");
      _ = t.CreateColumn("DocumentID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Title", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("FileName", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("FileExtension", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("Revision", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ChangeNumber", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Status", new SqlValueType(SqlType.UInt8));
      _ = t.CreateColumn("DocumentSummary", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("Document", new SqlValueType(SqlType.VarBinaryMax));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("Illustration");
      _ = t.CreateColumn("IllustrationID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Diagram", new SqlValueType("xml"));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("ProductDescription");
      _ = t.CreateColumn("ProductDescriptionID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Description", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("SpecialOffer");
      _ = t.CreateColumn("SpecialOfferID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Description", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("DiscountPct", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Type", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("Category", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("StartDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("EndDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("MinQty", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("MaxQty", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("ProductPhoto");
      _ = t.CreateColumn("ProductPhotoID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ThumbNailPhoto", new SqlValueType(SqlType.VarBinaryMax));
      _ = t.CreateColumn("ThumbnailPhotoFileName", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("LargePhoto", new SqlValueType(SqlType.VarBinaryMax));
      _ = t.CreateColumn("LargePhotoFileName", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("Customer");
      _ = t.CreateColumn("CustomerID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("TerritoryID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("AccountNumber", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("CustomerType", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("CustomerAddress");
      _ = t.CreateColumn("CustomerID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("AddressID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("AddressTypeID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["HumanResources"].CreateTable("EmployeeAddress");
      _ = t.CreateColumn("EmployeeID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("AddressID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Purchasing"].CreateTable("VendorAddress");
      _ = t.CreateColumn("VendorID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("AddressID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("AddressTypeID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Purchasing"].CreateTable("ProductVendor");
      _ = t.CreateColumn("ProductID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("VendorID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("AverageLeadTime", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("StandardPrice", new SqlValueType("money"));
      _ = t.CreateColumn("LastReceiptCost", new SqlValueType("money"));
      _ = t.CreateColumn("LastReceiptDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("MinOrderQty", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("MaxOrderQty", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("OnOrderQty", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("UnitMeasureCode", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("BillOfMaterials");
      _ = t.CreateColumn("BillOfMaterialsID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ProductAssemblyID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ComponentID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("StartDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("EndDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("UnitMeasureCode", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("BOMLevel", new SqlValueType(SqlType.Int16));
      _ = t.CreateColumn("PerAssemblyQty", new SqlValueType(SqlType.Decimal));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Purchasing"].CreateTable("PurchaseOrderHeader");
      _ = t.CreateColumn("PurchaseOrderID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("RevisionNumber", new SqlValueType(SqlType.UInt8));
      _ = t.CreateColumn("Status", new SqlValueType(SqlType.UInt8));
      _ = t.CreateColumn("EmployeeID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("VendorID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ShipMethodID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("OrderDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("ShipDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("SubTotal", new SqlValueType("money"));
      _ = t.CreateColumn("TaxAmt", new SqlValueType("money"));
      _ = t.CreateColumn("Freight", new SqlValueType("money"));
      _ = t.CreateColumn("TotalDue", new SqlValueType("money"));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Purchasing"].CreateTable("VendorContact");
      _ = t.CreateColumn("VendorID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ContactID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ContactTypeID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("ContactCreditCard");
      _ = t.CreateColumn("ContactID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("CreditCardID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("StoreContact");
      _ = t.CreateColumn("CustomerID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ContactID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ContactTypeID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Purchasing"].CreateTable("PurchaseOrderDetail");
      _ = t.CreateColumn("PurchaseOrderID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("PurchaseOrderDetailID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("DueDate", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("OrderQty", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ProductID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("UnitPrice", new SqlValueType(SqlType.Int32));
      c = t.CreateColumn("LineTotal");
      SqlTableRef pod = SqlDml.TableRef(t);
      c.Expression = pod["OrderQty"] * pod["UnitPrice"];
      c.IsPersisted = false;
      _ = t.CreateColumn("ReceivedQty", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("RejectedQty", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("StockedQty", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("WorkOrderRouting");
      _ = t.CreateColumn("WorkOrderID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ProductID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("OperationSequence", new SqlValueType(SqlType.Int16));
      _ = t.CreateColumn("LocationID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ScheduledStartDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("ScheduledEndDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("ActualStartDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("ActualEndDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("ActualResourceHrs", new SqlValueType(SqlType.Decimal));
      _ = t.CreateColumn("PlannedCost", new SqlValueType("money"));
      _ = t.CreateColumn("ActualCost", new SqlValueType("money"));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("CountryRegionCurrency");
      _ = t.CreateColumn("CountryRegionCode", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("CurrencyCode", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("ProductModelProductDescriptionCulture");
      _ = t.CreateColumn("ProductModelID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ProductDescriptionID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("CultureID", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("CurrencyRate");
      _ = t.CreateColumn("CurrencyRateID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("CurrencyRateDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("FromCurrencyCode", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ToCurrencyCode", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("AverageRate", new SqlValueType("money"));
      _ = t.CreateColumn("EndOfDayRate", new SqlValueType("money"));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("SalesOrderDetail");
      _ = t.CreateColumn("SalesOrderID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("SalesOrderDetailID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("CarrierTrackingNumber", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("OrderQty", new SqlValueType(SqlType.Int16));
      _ = t.CreateColumn("ProductID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("SpecialOfferID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("UnitPrice", new SqlValueType("money"));
      _ = t.CreateColumn("UnitPriceDiscount", new SqlValueType("money"));
      _ = t.CreateColumn("LineTotal", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("SalesOrderHeaderSalesReason");
      _ = t.CreateColumn("SalesOrderID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("SalesReasonID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["HumanResources"].CreateTable("EmployeeDepartmentHistory");
      _ = t.CreateColumn("EmployeeID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("DepartmentID", new SqlValueType(SqlType.Int16));
      _ = t.CreateColumn("ShiftID", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("StartDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("EndDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("ProductDocument");
      _ = t.CreateColumn("ProductID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("DocumentID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["HumanResources"].CreateTable("EmployeePayHistory");
      _ = t.CreateColumn("EmployeeID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("RateChangeDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("Rate", new SqlValueType("money"));
      _ = t.CreateColumn("PayFrequency", new SqlValueType(SqlType.UInt8));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("SalesPerson");
      _ = t.CreateColumn("SalesPersonID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("TerritoryID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("SalesQuota", new SqlValueType("money"));
      _ = t.CreateColumn("Bonus", new SqlValueType("money"));
      _ = t.CreateColumn("CommissionPct", new SqlValueType("money"));
      _ = t.CreateColumn("SalesYTD", new SqlValueType("money"));
      _ = t.CreateColumn("SalesLastYear", new SqlValueType("money"));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("SalesPersonQuotaHistory");
      _ = t.CreateColumn("SalesPersonID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("QuotaDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("SalesQuota", new SqlValueType("money"));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("SalesTerritoryHistory");
      _ = t.CreateColumn("SalesPersonID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("TerritoryID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("StartDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("EndDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("ProductModelIllustration");
      _ = t.CreateColumn("ProductModelID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("IllustrationID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("ProductInventory");
      _ = t.CreateColumn("ProductID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("LocationID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Shelf", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("Bin", new SqlValueType(SqlType.UInt8));
      _ = t.CreateColumn("Quantity", new SqlValueType(SqlType.UInt8));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("WorkOrder");
      _ = t.CreateColumn("WorkOrderID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ProductID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("OrderQty", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("StockedQty", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ScrappedQty", new SqlValueType(SqlType.Int16));
      _ = t.CreateColumn("StartDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("EndDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("DueDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("ScrapReasonID", new SqlValueType(SqlType.Int16));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("TransactionHistory");
      _ = t.CreateColumn("TransactionID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ProductID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ReferenceOrderID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ReferenceOrderLineID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("TransactionDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("TransactionType", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("Quantity", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ActualCost", new SqlValueType("money"));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("ShoppingCartItem");
      _ = t.CreateColumn("ShoppingCartItemID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ShoppingCartID", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("Quantity", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ProductID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("DateCreated", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("ProductListPriceHistory");
      _ = t.CreateColumn("ProductID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("StartDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("EndDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("ListPrice", new SqlValueType("money"));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("SpecialOfferProduct");
      _ = t.CreateColumn("SpecialOfferID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ProductID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("ProductCostHistory");
      _ = t.CreateColumn("ProductID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("StartDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("EndDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("StandardCost", new SqlValueType("money"));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Person"].CreateTable("Address");
      _ = t.CreateColumn("AddressID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("AddressLine1", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("AddressLine2", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("City", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("StateProvinceID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("PostalCode", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Purchasing"].CreateTable("Vendor");
      _ = t.CreateColumn("VendorID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("AccountNumber", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("Name", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("CreditRating", new SqlValueType(SqlType.UInt8));
      _ = t.CreateColumn("PreferredVendorStatus", new SqlValueType(SqlType.Boolean));
      _ = t.CreateColumn("ActiveFlag", new SqlValueType(SqlType.Boolean));
      _ = t.CreateColumn("PurchasingWebServiceURL", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("SalesOrderHeader");
      _ = t.CreateColumn("SalesOrderID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("RevisionNumber", new SqlValueType(SqlType.UInt8));
      _ = t.CreateColumn("OrderDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("DueDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("ShipDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("Status", new SqlValueType(SqlType.UInt8));
      _ = t.CreateColumn("OnlineOrderFlag", new SqlValueType(SqlType.Boolean));
      _ = t.CreateColumn("SalesOrderNumber", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("PurchaseOrderNumber", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("AccountNumber", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("CustomerID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ContactID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("SalesPersonID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("TerritoryID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("BillToAddressID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ShipToAddressID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ShipMethodID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("CreditCardID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("CreditCardApprovalCode", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("CurrencyRateID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("SubTotal", new SqlValueType("money"));
      _ = t.CreateColumn("TaxAmt", new SqlValueType("money"));
      _ = t.CreateColumn("Freight", new SqlValueType("money"));
      _ = t.CreateColumn("TotalDue", new SqlValueType("money"));
      _ = t.CreateColumn("Comment", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["HumanResources"].CreateTable("Employee");
      _ = t.CreateColumn("EmployeeID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("NationalIDNumber", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ContactID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("LoginID", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ManagerID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Title", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("BirthDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("MaritalStatus", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("Gender", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("HireDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("SalariedFlag", new SqlValueType(SqlType.Boolean));
      _ = t.CreateColumn("VacationHours", new SqlValueType(SqlType.Int16));
      _ = t.CreateColumn("SickLeaveHours", new SqlValueType(SqlType.Int16));
      _ = t.CreateColumn("CurrentFlag", new SqlValueType(SqlType.Boolean));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("ProductProductPhoto");
      _ = t.CreateColumn("ProductID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ProductPhotoID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Primary", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Person"].CreateTable("StateProvince");
      _ = t.CreateColumn("StateProvinceID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("StateProvinceCode", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("CountryRegionCode", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("IsOnlyStateProvinceFlag", new SqlValueType(SqlType.Boolean));
      _ = t.CreateColumn("Name", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("TerritoryID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("ProductModel");
      _ = t.CreateColumn("ProductModelID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Name", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("CatalogDescription", new SqlValueType("xml"));
      _ = t.CreateColumn("Instructions", new SqlValueType("xml"));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("Product");
      c = t.CreateColumn("ProductID", new SqlValueType(SqlType.Int32));
      c.SequenceDescriptor = new SequenceDescriptor(c, 1, 1);
      c.IsNullable = false;
      c = t.CreateColumn("Name", new SqlValueType(SqlType.VarChar, 50));
      c.IsNullable = false;
      c = t.CreateColumn("ProductNumber", new SqlValueType(SqlType.VarChar, 25));
      c.Collation = catalog.Schemas["Production"].Collations["SQL_Latin1_General_CP1_CI_AS"];
      c.IsNullable = false;
      c = t.CreateColumn("MakeFlag", new SqlValueType(SqlType.Boolean));
      c.DefaultValue = 1;
      c.IsNullable = false;
      c = t.CreateColumn("FinishedGoodsFlag", new SqlValueType(SqlType.Boolean));
      c.DefaultValue = 1;
      c.IsNullable = false;
      c = t.CreateColumn("Color", new SqlValueType(SqlType.VarChar, 15));
      c.Collation = catalog.Schemas["Production"].Collations["SQL_Latin1_General_CP1_CI_AS"];
      c = t.CreateColumn("SafetyStockLevel", new SqlValueType(SqlType.Int16));
      c.IsNullable = false;
      c = t.CreateColumn("ReorderPoint", new SqlValueType(SqlType.Int16));
      c.IsNullable = false;
      c = t.CreateColumn("StandardCost", new SqlValueType("money"));
      c.IsNullable = false;
      c = t.CreateColumn("ListPrice", new SqlValueType("money"));
      c.IsNullable = false;
      c = t.CreateColumn("Size", new SqlValueType(SqlType.VarChar, 5));
      c.Collation = catalog.Schemas["Production"].Collations["SQL_Latin1_General_CP1_CI_AS"];
      c = t.CreateColumn("SizeUnitMeasureCode", new SqlValueType(SqlType.Char, 3));
      c.Collation = catalog.Schemas["Production"].Collations["SQL_Latin1_General_CP1_CI_AS"];
      c = t.CreateColumn("WeightUnitMeasureCode", new SqlValueType(SqlType.Char, 3));
      c.Collation = catalog.Schemas["Production"].Collations["SQL_Latin1_General_CP1_CI_AS"];
      _ = t.CreateColumn("Weight", new SqlValueType(SqlType.Decimal, 8, 2));
      c = t.CreateColumn("DaysToManufacture", new SqlValueType(SqlType.Int32));
      c.IsNullable = false;
      c = t.CreateColumn("ProductLine", new SqlValueType(SqlType.Char, 2));
      c.Collation = catalog.Schemas["Production"].Collations["SQL_Latin1_General_CP1_CI_AS"];
      c = t.CreateColumn("Class", new SqlValueType(SqlType.Char, 2));
      c.Collation = catalog.Schemas["Production"].Collations["SQL_Latin1_General_CP1_CI_AS"];
      c = t.CreateColumn("Style", new SqlValueType(SqlType.Char, 2));
      c.Collation = catalog.Schemas["Production"].Collations["SQL_Latin1_General_CP1_CI_AS"];
      _ = t.CreateColumn("ProductSubcategoryID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ProductModelID", new SqlValueType(SqlType.Int32));
      c = t.CreateColumn("SellStartDate", new SqlValueType(SqlType.DateTime));
      c.IsNullable = false;
      _ = t.CreateColumn("SellEndDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("DiscontinuedDate", new SqlValueType(SqlType.DateTime));
      c = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));
      c.DefaultValue = SqlDml.CurrentDate();
      c.IsNullable = false;
      var st = SqlDml.TableRef(t);
      _ = t.CreateCheckConstraint("CK_Product_Class", SqlDml.Upper(st["Class"]) == 'H' ||
        SqlDml.Upper(st["Class"]) == 'M' ||
          SqlDml.Upper(st["Class"]) == 'L' ||
            SqlDml.IsNull(st["Class"]));
      _ = t.CreateCheckConstraint("CK_Product_DaysToManufacture", SqlDml.Upper(st["DaysToManufacture"]) >= 0);
      _ = t.CreateCheckConstraint("CK_Product_ListPrice", SqlDml.Upper(st["ListPrice"]) >= 0);
      _ = t.CreateCheckConstraint("CK_Product_ProductLine", SqlDml.Upper(st["ProductLine"]) == 'R' ||
        SqlDml.Upper(st["ProductLine"]) == 'M' ||
          SqlDml.Upper(st["ProductLine"]) == 'T' ||
            SqlDml.Upper(st["ProductLine"]) == 'S' ||
              SqlDml.IsNull(st["ProductLine"]));
      _ = t.CreateCheckConstraint("CK_Product_ReorderPoint", SqlDml.Upper(st["ReorderPoint"]) > 0);
      _ = t.CreateCheckConstraint("CK_Product_SafetyStockLevel", SqlDml.Upper(st["SafetyStockLevel"]) > 0);
      _ = t.CreateCheckConstraint("CK_Product_SellEndDate", SqlDml.Upper(st["SellEndDate"]) > st["SellStartDate"] ||
        SqlDml.IsNull(st["SellEndDate"]));
      _ = t.CreateCheckConstraint("CK_Product_StandardCost", SqlDml.Upper(st["StandardCost"]) >= 0);
      _ = t.CreateCheckConstraint("CK_Product_Style", SqlDml.Upper(st["Style"]) == 'U' ||
        SqlDml.Upper(st["Style"]) == 'M' ||
          SqlDml.Upper(st["Style"]) == 'W' ||
            SqlDml.IsNull(st["Style"]));
      _ = t.CreateCheckConstraint("CK_Product_Weight", SqlDml.Upper(st["Weight"]) > 0);
      _ = t.CreatePrimaryKey("PK_Product_ProductID", t.TableColumns["ProductID"]);
      cs = t.CreateForeignKey("FK_Product_ProductModel_ProductModelID");
      ((ForeignKey)cs).Columns.Add(t.TableColumns["ProductModelID"]);
      ((ForeignKey)cs).ReferencedColumns.Add(catalog.Schemas["Production"].Tables["ProductModel"].TableColumns["ProductModelID"]);

      t = catalog.Schemas["Person"].CreateTable("Contact");
      _ = t.CreateColumn("ContactID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("NameStyle", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Title", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("FirstName", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("MiddleName", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("LastName", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("Suffix", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("EmailAddress", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("EmailPromotion", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Phone", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("PasswordHash", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("PasswordSalt", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("AdditionalContactInfo", new SqlValueType("xml"));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("UnitMeasure");
      _ = t.CreateColumn("UnitMeasureCode", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("Name", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("ProductReview");
      _ = t.CreateColumn("ProductReviewID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ProductID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ReviewerName", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ReviewDate", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("EmailAddress", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("Rating", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Comments", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("ProductSubcategory");
      _ = t.CreateColumn("ProductSubcategoryID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ProductCategoryID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Name", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Person"].CreateTable("AddressType");
      _ = t.CreateColumn("AddressTypeID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Name", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("SalesReason");
      _ = t.CreateColumn("SalesReasonID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Name", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ReasonType", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["HumanResources"].CreateTable("Department");
      _ = t.CreateColumn("DepartmentID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Name", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("GroupName", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Person"].CreateTable("CountryRegion");
      _ = t.CreateColumn("CountryRegionCode", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("Name", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("Culture");
      _ = t.CreateColumn("CultureID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Name", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("Currency");
      _ = t.CreateColumn("CurrencyCode", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("Name", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Person"].CreateTable("ContactType");
      _ = t.CreateColumn("ContactTypeID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Name", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("SalesTaxRate");
      _ = t.CreateColumn("SalesTaxRateID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("StateProvinceID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("TaxType", new SqlValueType(SqlType.UInt8));
      _ = t.CreateColumn("TaxRate", new SqlValueType("money"));
      _ = t.CreateColumn("Name", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("Location");
      _ = t.CreateColumn("LocationID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Name", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("CostRate", new SqlValueType("money"));
      _ = t.CreateColumn("Availability", new SqlValueType(SqlType.Decimal));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("SalesTerritory");
      _ = t.CreateColumn("TerritoryID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Name", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("CountryRegionCode", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("Group", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("TaxRate", new SqlValueType("money"));
      _ = t.CreateColumn("SalesYTD", new SqlValueType("money"));
      _ = t.CreateColumn("SalesLastYear", new SqlValueType("money"));
      _ = t.CreateColumn("CostYTD", new SqlValueType("money"));
      _ = t.CreateColumn("CostLastYear", new SqlValueType("money"));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("ScrapReason");
      _ = t.CreateColumn("ScrapReasonID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Name", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["HumanResources"].CreateTable("Shift");
      _ = t.CreateColumn("ShiftID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Name", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("StartTime", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("EndTime", new SqlValueType(SqlType.DateTime));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Production"].CreateTable("ProductCategory");
      _ = t.CreateColumn("ProductCategoryID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Name", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Purchasing"].CreateTable("ShipMethod");
      _ = t.CreateColumn("ShipMethodID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Name", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("ShipBase", new SqlValueType("money"));
      _ = t.CreateColumn("ShipRate", new SqlValueType("money"));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("Store");
      _ = t.CreateColumn("CustomerID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Name", new SqlValueType(SqlType.VarChar));
      _ = t.CreateColumn("SalesPersonID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Demographics", new SqlValueType("xml"));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["Sales"].CreateTable("Individual");
      _ = t.CreateColumn("CustomerID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("ContactID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Demographics", new SqlValueType("xml"));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      t = catalog.Schemas["HumanResources"].CreateTable("JobCandidate");
      _ = t.CreateColumn("JobCandidateID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("EmployeeID", new SqlValueType(SqlType.Int32));
      _ = t.CreateColumn("Resume", new SqlValueType("xml"));
      _ = t.CreateColumn("ModifiedDate", new SqlValueType(SqlType.DateTime));

      return catalog;
    }

    private string Compile(ISqlCompileUnit compileUnit)
    {
      return sqlDriver.Compile(compileUnit).GetCommandText();
    }

    public MSSQLQueries()
    {
      Catalog = CreateCatalogInMemory();
      var factory = new Xtensive.Sql.Drivers.SqlServer.DriverFactory();
      sqlDriver = factory.GetDriver(new Xtensive.Orm.ConnectionInfo(ConnectionUrl));

      var queriesCountToCache = typeof(MSSQLQueries).GetMethods(
        System.Reflection.BindingFlags.Instance
        | System.Reflection.BindingFlags.Public
        | System.Reflection.BindingFlags.DeclaredOnly).Length;

      queriesCache = new ConcurrentDictionary<string, ISqlCompileUnit>(Environment.ProcessorCount, queriesCountToCache, StringComparer.Ordinal);
    }
  }
}
