using Conduit.MySQL;
using Conduit.MySQL.Enums;
using Conduit.MySQL.Models;
using Conduit.MySQL.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace Conduit.Test.Conduit.MySQL
{
    public class DatabaseHelper: IDisposable
    {
        private readonly string _connectionString = "server=127.0.0.1;user id=conduitadmin;password=conduitpass;port=52525;database=conduitdb;";
        private bool _isTipPresent;

        public DatabaseHelper()
        {
            using (var database = new ApplicationDatabase(_connectionString))
            {
                var service = new RestorationService(database);
                var transactionHash = Convert.FromHexString("1afaa1c87ca193480c9aa176f08af78e457e8b8415c71697eded1297ed953db6").Reverse().ToArray();
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

            var results = await _service.GetPushDataFilterMatches(new PushDataFilter
            {
                FilterKeys = new List<byte[]> {
                    // This is a garbage value.
                    Convert.FromHexString("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                }
            });
            Assert.Empty(results);
        }

        /// <summary>
        /// Find the funding of a P2PK UTXO, and the subsequent spend information.
        /// </summary>
        [Fact]
        public async void TestP2PK()
        {
            Assert.True(_dbHelper.FoundValidChain, "Unable to find blockchain_115_3677f4 data");

            var results = await _service.GetPushDataFilterMatches(new PushDataFilter
            {
                FilterKeys = new List<byte[]> {
                    // This is the SHA256 checksum of the public key bytes from '032fcb2fa3280cfdc0ffd527b40f592f5ae80556f2c9f98a649f1b1af13f332fdb'.
                    Convert.FromHexString("04bca2ae277997940152716854a95347819c2e07d370d22c093b39708fb9d5eb"),
                }
            });
            Assert.Single(results);
            var match = results[0];
            Assert.Equal("88c92bb09626c7d505ed861ae8fa7e7aaab5b816fc517eac7a8a6c7f28b1b210", Convert.ToHexString(match.TransactionHash.Reverse().ToArray()).ToLower());
            Assert.Equal(0, match.Index);
            Assert.Equal(TransactionReferenceType.Output, match.ReferenceType);
            Assert.Equal("47f3f47a256d70950ff5690ea377c24464310489e3f54d01b817dd0088f0a095", Convert.ToHexString(match.SpendTransactionHash.Reverse().ToArray()).ToLower());
            Assert.Equal(0, match.SpendInputIndex);
        }

        /// <summary>
        /// Find the funding of a P2PKH UTXO, and the subsequent spend information.
        /// </summary>
        [Fact]
        public async void TestP2PKH()
        {
            Assert.True(_dbHelper.FoundValidChain, "Unable to find blockchain_115_3677f4 data");

            var results = await _service.GetPushDataFilterMatches(new PushDataFilter
            {
                FilterKeys = new List<byte[]> {
                    // This is the SHA256 checksum of the hash160 of the P2PKH address 'mhg6ENXhPL6LsEUG6oxdqi8LjE2bsW6NMW'.
                    Convert.FromHexString("e351e4d2499786e8a3ac5468cbf1444b3416b41e424524b50e2dafc8f6f454db"),
                }
            });
            Assert.Single(results);
            var match = results[0];
            Assert.Equal("d53a9ebfac748561132e49254c42dbe518080c2a5956822d5d3914d47324e842", Convert.ToHexString(match.TransactionHash.Reverse().ToArray()).ToLower());
            Assert.Equal(0, match.Index);
            Assert.Equal(TransactionReferenceType.Output, match.ReferenceType);
            Assert.Equal("47f3f47a256d70950ff5690ea377c24464310489e3f54d01b817dd0088f0a095", Convert.ToHexString(match.SpendTransactionHash.Reverse().ToArray()).ToLower());
            Assert.Equal(1, match.SpendInputIndex);
        }

        /// <summary>
        /// Find the funding of a P2SH UTXO, and the subsequent spend information.
        /// </summary>
        [Fact]
        public async void TestP2SH()
        {
            Assert.True(_dbHelper.FoundValidChain, "Unable to find blockchain_115_3677f4 data");

            var results = await _service.GetPushDataFilterMatches(new PushDataFilter {
                FilterKeys = new List<byte[]> {
                    // This is the SHA256 checksum of the hash160 of the P2SH address '2N5338aAPYmKM59AKpvxDB6FRAnGNXRsfBp'.
                    Convert.FromHexString("5e7583878789b03276d2d60a1cf3772a999084e3b12d0d3c1a33a30bd15609db"),
                }
            });
            Assert.Single(results);
            var match = results[0];
            Assert.Equal("49250a55f59e2bbf1b0615508c2d586c1336d7c0c6d493f02bc82349fabe6609", Convert.ToHexString(match.TransactionHash.Reverse().ToArray()).ToLower());
            Assert.Equal(1, match.Index);
            Assert.Equal(TransactionReferenceType.Output, match.ReferenceType);
            Assert.Equal("1afaa1c87ca193480c9aa176f08af78e457e8b8415c71697eded1297ed953db6", Convert.ToHexString(match.SpendTransactionHash.Reverse().ToArray()).ToLower());
            Assert.Equal(0, match.SpendInputIndex);
        }

        /// <summary>
        /// Find the funding of a P2MS (bare multi-signature) UTXO, and the subsequent spend information (from cosigner 1 perspective).
        /// </summary>
        [Fact]
        public async void TestP2MS1()
        {
            Assert.True(_dbHelper.FoundValidChain, "Unable to find blockchain_115_3677f4 data");

            var results = await _service.GetPushDataFilterMatches(new PushDataFilter
            {
                FilterKeys = new List<byte[]> {
                    // This is the SHA256 checksum of the cosigner 1 0/0' child public key.
                    Convert.FromHexString("9ed50dfe0d3a28950ee9a2ee41dce7193dd8666c4ff42c974de1bde60332a701"),
                }
            });
            Assert.Single(results);
            var match = results[0];
            Assert.Equal("479833ff49d1000cd6f9d23a88924d22eaeae8b9d543e773d7420c2bbfd73fe2", Convert.ToHexString(match.TransactionHash.Reverse().ToArray()).ToLower());
            Assert.Equal(2, match.Index);
            Assert.Equal(TransactionReferenceType.Output, match.ReferenceType);
            Assert.Equal("0120eae6dc11459fe79fbad26f998f4f8c5b75fa6f0fff5b0beca4f35ea7d721", Convert.ToHexString(match.SpendTransactionHash.Reverse().ToArray()).ToLower());
            Assert.Equal(0, match.SpendInputIndex);
        }

        /// <summary>
        /// Find the funding of a P2MS (bare multi-signature) UTXO, and the subsequent spend information (from co-signer 2 perspective).
        /// </summary>
        [Fact]
        public async void TestP2MS2()
        {
            Assert.True(_dbHelper.FoundValidChain, "Unable to find blockchain_115_3677f4 data");

            var results = await _service.GetPushDataFilterMatches(new PushDataFilter
            {
                FilterKeys = new List<byte[]> {
                    // This is the SHA256 checksum of the cosigner 2 0/0' child public key.
                    Convert.FromHexString("e6221c70e0f3c686255b548789c63d0e2c6aa795ad87324dfd71d0b53d90d59d"),
                }
            });
            Assert.Single(results);
            var match = results[0];
            Assert.Equal("479833ff49d1000cd6f9d23a88924d22eaeae8b9d543e773d7420c2bbfd73fe2", Convert.ToHexString(match.TransactionHash.Reverse().ToArray()).ToLower());
            Assert.Equal(2, match.Index);
            Assert.Equal(TransactionReferenceType.Output, match.ReferenceType);
            Assert.Equal("0120eae6dc11459fe79fbad26f998f4f8c5b75fa6f0fff5b0beca4f35ea7d721", Convert.ToHexString(match.SpendTransactionHash.Reverse().ToArray()).ToLower());
            Assert.Equal(0, match.SpendInputIndex);
        }

        public void Dispose()
        {
            _database.Dispose();
        }
    }
}
