using Conduit.MySQL;
using Conduit.MySQL.Enums;
using Conduit.MySQL.Models;
using Conduit.MySQL.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Conduit.Test.Conduit.MySQL
{
    /// <summary>
    /// Conversion utility functions
    /// </summary>>
    class HashConversions
    {
        public static byte[] HashToHashX(byte[] fullHash)
        {
            return fullHash.Take(14).ToArray();
        }
        public static byte[] HexStringToHashX(string hexString)
        {
            // Use for tx hashes which need to be reversed
            return HashToHashX(Convert.FromHexString(hexString).ToArray().Reverse().ToArray());
        }
        
        public static byte[] PushdataHexStringToHashX(string hexString)
        {
            // Use for pushdata hashes which are NOT reversed
            return HashToHashX(Convert.FromHexString(hexString));
        }
    }

    /// <summary>
    /// You need to manually write your own comparer for byte arrays because.. I do not know.
    /// </summary>
    class ByteArrayEqualityComparer : IEqualityComparer<byte[]>
    {
        public bool Equals(byte[] x, byte[] y)
        {
            if (ReferenceEquals(x, y)) return true;
            if (x == null || y == null) return false;
            if (x.Length != y.Length) return false;
            for (int i = 0; i < x.Length; i++)
            {
                if (x[i] != y[i]) return false;
            }
            return true;
        }

        public int GetHashCode(byte[] obj)
        {
            int result = 13 * obj.Length;
            for (int i = 0; i < obj.Length; i++)
            {
                result = (17 * result) + obj[i];
            }
            return result;
        }
    }

    public class DatabaseHelper: IDisposable
    {
        private readonly string _connectionString = "server=127.0.0.1;user id=conduitadmin;password=conduitpass;port=52525;database=conduitdb;";
        private bool _isTipPresent;

        public DatabaseHelper()
        {
            using (var database = new ApplicationDatabase(_connectionString))
            {
                var service = new RestorationService(database);
                var transactionHash = HashConversions.HexStringToHashX("1afaa1c87ca193480c9aa176f08af78e457e8b8415c71697eded1297ed953db6");
                var transactionHeight = service.GetTransactionHeightSync(transactionHash);
                _isTipPresent = transactionHeight == 115;
            }
        }

        public bool FoundValidChain { get { return _isTipPresent;  } }

        public ApplicationDatabase ApplicationDatabase {
            get {
                return new ApplicationDatabase(_connectionString);
            }
        }

        public void Dispose()
        {
        }
    }

    public class TestRestorationService : IClassFixture<DatabaseHelper>, IDisposable
    {
        private XunitLogger<RestorationService> _logger;
        private ApplicationDatabase _database;
        private IRestorationService _service;
        private DatabaseHelper _dbHelper;


        public TestRestorationService(ITestOutputHelper output, DatabaseHelper dbHelper)
        {
            _logger = new XunitLogger<RestorationService>(output);
            _dbHelper = dbHelper;

            _database = dbHelper.ApplicationDatabase;
            _service = new RestorationService(_database);
        }

        /// <summary>
        /// Find no match for something that is not there.
        /// </summary>
        [Fact]
        public async void TestNonMatch()
        {
            Assert.True(_dbHelper.FoundValidChain, "Unable to find blockchain_115_3677f4 data");
            var results = new List<PushDataFilterMatch>();
            var pd_filter = new PushDataFilter
            {
                FilterKeys = new List<byte[]>
                {
                    // This is a garbage value.
                    HashConversions.PushdataHexStringToHashX("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                }
            };
            await foreach (var pushdata_match in _service.GetPushDataFilterMatches(pd_filter)){
                results.Add(pushdata_match);
            }
            Assert.Empty(results);
        }

        /// <summary>
        /// Check the coinbase transactions going to the mining wallet. This checks the unspent and spent UTXOs are correct.
        /// </summary>
        [Fact]
        public async void TestMiningTransactions()
        {
            Assert.True(_dbHelper.FoundValidChain, "Unable to find blockchain_115_3677f4 data");
            var results = new List<PushDataFilterMatch>();
            var pd_filter = new PushDataFilter
            {
                FilterKeys = new List<byte[]>
                {
                    // This is the SHA256 checksum of the hash160 of the P2PKH address 'n2ekqiw96ceQWFrKSziKTEi5fsRuZKQdun'.
                    Convert.FromHexString("86c73b803ee5229044621b2fb6fb61b7001a92cbfdab1c7314da27a2fee72948"),
                }
            };
            _logger.LogDebug($"pd_filter: {Convert.ToHexString(pd_filter.FilterKeys[0])}");
            await foreach (var pushdata_match in _service.GetPushDataFilterMatches(pd_filter)){
                results.Add(pushdata_match);
            }
            Assert.Equal(110, results.Count);

            var unspentCoinbaseTransactionHashes = results.Where(w => w.SpendTransactionHash == null).Select(s => s.TransactionHash).Distinct().ToList();
            Assert.Equal(100, unspentCoinbaseTransactionHashes.Count);

            var spentResultsEnumerable = results.Where(w => w.SpendTransactionHash != null && w.SpendInputIndex != -1);
            var spentCoinbaseTransactionHashes = spentResultsEnumerable.Select(s => s.TransactionHash).ToHashSet(new ByteArrayEqualityComparer());
            Assert.Equal(10, spentCoinbaseTransactionHashes.Count);
            // Verify all the spent coinbase UTXOs are spent in the same transaction. This was where ElectrumSV made change from the 10 (of 110) matured coinbase UTXOs.
            Assert.Single(spentResultsEnumerable.Select(s => s.SpendTransactionHash).ToHashSet(new ByteArrayEqualityComparer()));
            // Verify all the input indexes for the spent coinbases are different.
            Assert.Equal(10, spentResultsEnumerable.Select(s => s.SpendInputIndex).Distinct().Count());

            var expectedSpentCoinbaseTransactionHashes = new List<string>
            {
                "FCD363867BAB384A2CCB4349AEF3EE173D965561CA574B89E9FDB76642DD4D2B",
                "59D06760245723B17BFFD9D587EC01ACDFCA1B7F1ACA9184112EB615D8D50A70",
                "32EFB2AFDC5993AA3D63DBE031196B2AC08BFF196B3FF259DD50F5FB7A4F2CE0",
                "F90E0A8B2667BFC9BB19D2EAC8CF48F78F00F8FA5CA168591E3C1B0346203004",
                "3D052E3F9DF5073A04298AD87B01E6DC186665E4E4D7F965E210723DEE56E2E0",
                "19F2B7FFA0D44E15E6568572590A2D4CBCC80B64859CFCBDCB1260DD5E4383F6",
                "59E863F3BB2F1EC7192529513B95B4A782C80CDC930B028A9D53343866BF5641",
                "1008FD90BB1055AF8AF5272BB60B0E11FE34B777983156714D7DFD2695393513",
                "EE70715C37F23D72803A904A142AE483CE1E776278304DB975846378FFE99437",
                "9E724D5DE860799E909C2B94858741033C2E4201037F860BB583136CBEAB97D8",
            }.Select(s => Convert.FromHexString(s)).ToHashSet(new ByteArrayEqualityComparer());
            Assert.True(expectedSpentCoinbaseTransactionHashes.SetEquals(spentCoinbaseTransactionHashes));
        }

        /// <summary>
        /// Find the funding of a P2PK UTXO, and the subsequent spend information.
        /// </summary>
        [Fact]
        public async void TestP2PK()
        {
            Assert.True(_dbHelper.FoundValidChain, "Unable to find blockchain_115_3677f4 data");
            var results = new List<PushDataFilterMatch>();
            var pd_filter = new PushDataFilter
            {
                FilterKeys = new List<byte[]>
                {
                    // This is the SHA256 checksum of the hash160 of the P2PKH address 'n2ekqiw96ceQWFrKSziKTEi5fsRuZKQdun'.
                    Convert.FromHexString("04bca2ae277997940152716854a95347819c2e07d370d22c093b39708fb9d5eb"),
                }
            };
            await foreach (var pushdata_match in _service.GetPushDataFilterMatches(pd_filter)){
                results.Add(pushdata_match);
            }

            Assert.Single(results);
            var match = results[0];
            Assert.Equal(HashConversions.HexStringToHashX("88c92bb09626c7d505ed861ae8fa7e7aaab5b816fc517eac7a8a6c7f28b1b210"), match.TransactionHash.ToArray());
            Assert.StrictEqual<uint>(0, match.Index);
            Assert.Equal(TransactionReferenceType.Output, match.ReferenceType);
            Assert.Equal(HashConversions.HexStringToHashX("47f3f47a256d70950ff5690ea377c24464310489e3f54d01b817dd0088f0a095"), match.SpendTransactionHash.ToArray());
            Assert.StrictEqual<uint>(0, match.SpendInputIndex);
        }

        /// <summary>
        /// Find the funding of a P2PKH UTXO, and the subsequent spend information.
        /// </summary>
        [Fact]
        public async void TestP2PKH()
        {
            Assert.True(_dbHelper.FoundValidChain, "Unable to find blockchain_115_3677f4 data");
            var results = new List<PushDataFilterMatch>();
            var pd_filter = new PushDataFilter
            {
                FilterKeys = new List<byte[]>
                {
                    // This is the SHA256 checksum of the hash160 of the P2PKH address 'n2ekqiw96ceQWFrKSziKTEi5fsRuZKQdun'.
                    HashConversions.PushdataHexStringToHashX("e351e4d2499786e8a3ac5468cbf1444b3416b41e424524b50e2dafc8f6f454db"),
                }
            };
            await foreach (var pushdata_match in _service.GetPushDataFilterMatches(pd_filter)){
                results.Add(pushdata_match);
            }
            Assert.Single(results);
            var match = results[0];
            Assert.Equal(HashConversions.HexStringToHashX("d53a9ebfac748561132e49254c42dbe518080c2a5956822d5d3914d47324e842"), match.TransactionHash.ToArray());
            Assert.StrictEqual<uint>(0, match.Index);
            Assert.Equal(TransactionReferenceType.Output, match.ReferenceType);
            Assert.Equal(HashConversions.HexStringToHashX("47f3f47a256d70950ff5690ea377c24464310489e3f54d01b817dd0088f0a095"), match.SpendTransactionHash.ToArray());
            Assert.StrictEqual<uint>(1, match.SpendInputIndex);
        }

        /// <summary>
        /// Find the funding of a P2SH UTXO, and the subsequent spend information.
        /// </summary>
        [Fact]
        public async void TestP2SH()
        {
            Assert.True(_dbHelper.FoundValidChain, "Unable to find blockchain_115_3677f4 data");
            
            
            var results = new List<PushDataFilterMatch>();
            var pd_filter = new PushDataFilter
            {
                FilterKeys = new List<byte[]>
                {
                    // This is the SHA256 checksum of the hash160 of the P2SH address '2N5338aAPYmKM59AKpvxDB6FRAnGNXRsfBp'.
                    Convert.FromHexString("5e7583878789b03276d2d60a1cf3772a999084e3b12d0d3c1a33a30bd15609db"),
                }
            };
            await foreach (var pushdata_match in _service.GetPushDataFilterMatches(pd_filter)){
                results.Add(pushdata_match);
            }

            Assert.Single(results);
            var match = results[0];
            Assert.Equal(HashConversions.HexStringToHashX("49250a55f59e2bbf1b0615508c2d586c1336d7c0c6d493f02bc82349fabe6609"), match.TransactionHash.ToArray());
            Assert.StrictEqual<uint>(1, match.Index);
            Assert.Equal(TransactionReferenceType.Output, match.ReferenceType);
            Assert.Equal(HashConversions.HexStringToHashX("1afaa1c87ca193480c9aa176f08af78e457e8b8415c71697eded1297ed953db6"), match.SpendTransactionHash.ToArray());
            Assert.StrictEqual<uint>(0, match.SpendInputIndex);
        }

        /// <summary>
        /// Find the funding of a P2MS (bare multi-signature) UTXO, and the subsequent spend information (from cosigner 1 perspective).
        /// </summary>
        [Fact]
        public async void TestP2MS1()
        {
            Assert.True(_dbHelper.FoundValidChain, "Unable to find blockchain_115_3677f4 data");
            
            
            var results = new List<PushDataFilterMatch>();
            var pd_filter = new PushDataFilter
            {
                FilterKeys = new List<byte[]>
                {
                    // This is the SHA256 checksum of the hash160 of the P2PKH address 'n2ekqiw96ceQWFrKSziKTEi5fsRuZKQdun'.
                    Convert.FromHexString("9ed50dfe0d3a28950ee9a2ee41dce7193dd8666c4ff42c974de1bde60332a701"),
                }
            };
            await foreach (var pushdata_match in _service.GetPushDataFilterMatches(pd_filter)){
                results.Add(pushdata_match);
            }
            
            Assert.Single(results);
            var match = results[0];
            
            _logger.LogDebug($"Actual TxHash: {Convert.ToHexString(match.TransactionHash.Take(14).Reverse().ToArray()).ToLower()}");
            
            Assert.Equal(HashConversions.HexStringToHashX("479833ff49d1000cd6f9d23a88924d22eaeae8b9d543e773d7420c2bbfd73fe2"), match.TransactionHash.ToArray());
            Assert.StrictEqual<uint>(2, match.Index);
            Assert.Equal(TransactionReferenceType.Output, match.ReferenceType);
            Assert.Equal(HashConversions.HexStringToHashX("0120eae6dc11459fe79fbad26f998f4f8c5b75fa6f0fff5b0beca4f35ea7d721"), match.SpendTransactionHash.ToArray());
            Assert.StrictEqual<uint>(0, match.SpendInputIndex);
        }

        /// <summary>
        /// Find the funding of a P2MS (bare multi-signature) UTXO, and the subsequent spend information (from co-signer 2 perspective).
        /// </summary>
        [Fact]
        public async void TestP2MS2()
        {
            Assert.True(_dbHelper.FoundValidChain, "Unable to find blockchain_115_3677f4 data");
            
            var results = new List<PushDataFilterMatch>();
            var pd_filter = new PushDataFilter
            {
                FilterKeys = new List<byte[]>
                {
                    // This is the SHA256 checksum of the hash160 of the P2PKH address 'n2ekqiw96ceQWFrKSziKTEi5fsRuZKQdun'.
                    Convert.FromHexString("e6221c70e0f3c686255b548789c63d0e2c6aa795ad87324dfd71d0b53d90d59d"),
                }
            };
            await foreach (var pushdata_match in _service.GetPushDataFilterMatches(pd_filter)){
                results.Add(pushdata_match);
            }
            
            Assert.Single(results);
            var match = results[0];
            Assert.Equal(HashConversions.HexStringToHashX("479833ff49d1000cd6f9d23a88924d22eaeae8b9d543e773d7420c2bbfd73fe2"), match.TransactionHash);
            Assert.StrictEqual<uint>(2, match.Index);
            Assert.Equal(TransactionReferenceType.Output, match.ReferenceType);
            Assert.Equal(HashConversions.HexStringToHashX("0120eae6dc11459fe79fbad26f998f4f8c5b75fa6f0fff5b0beca4f35ea7d721"), match.SpendTransactionHash);
            Assert.StrictEqual<uint>(0, match.SpendInputIndex);
        }

        public void Dispose()
        {
            _database.Dispose();
        }
    }
}
