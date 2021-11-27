import logging
import requests

BASE_URL = f"http://127.0.0.1:34525"
PING_URL = BASE_URL + "/"
ERROR_URL = BASE_URL + "/error"
GET_TRANSACTION_URL = BASE_URL + "/api/v1/transaction/{txid}"


class TestInternalAiohttpRESTAPI:
    logger = logging.getLogger("TestInternalAiohttpRESTAPI")

    @classmethod
    def setup_class(klass) -> None:
        pass

    def setup_method(self) -> None:
        pass

    def teardown_method(self) -> None:
        pass

    @classmethod
    def teardown_class(klass) -> None:
        pass

    def test_ping(klass):
        result = requests.get(PING_URL)
        assert result.json() is True

    def test_error(klass):
        result = requests.get(ERROR_URL)
        assert result.status_code == 400
        assert result.reason is not None
        assert isinstance(result.reason, str)

    def test_get_transaction_json(klass):
        headers = {'Accept': "application/json"}
        txid = "88c92bb09626c7d505ed861ae8fa7e7aaab5b816fc517eac7a8a6c7f28b1b210"
        result = requests.get(GET_TRANSACTION_URL.format(txid=txid), headers=headers)
        assert result.status_code == 200
        assert result.json() == '010000000142e82473d414395d2d8256592a0c0818e5db424c25492e13618574acbf9e3ad5010000006a4730440220588dfde8f7ebcd4061c07eed09f244ee8fb81356589b60a2642be7a446a5b9e702203658bb40555624571444a6fe3cdeb65fdf2e58a8fd25962e7af3c0da68b13ea6412103faa0301e0659a4183f7c99605330d6355f58daa776a79eced758bafee47c6de6ffffffff0b80778e06000000002321032fcb2fa3280cfdc0ffd527b40f592f5ae80556f2c9f98a649f1b1af13f332fdbac80397a12000000001976a914e5f9cc3acf7199660c2edee11505f93eec14d2a088ac00d01213000000001976a9142feb5264b8add4ef9819a7302975db4636e50d4f88ac00d01213000000001976a9146f706feeaa8dcb6bf897b6849e4438b0f60ac7b888ac8066ab13000000001976a914aecc71efe7715b04eb851f8c62f3cfaeb837f60188ac54a45615000000001976a914d123bfc2581f2b8b8f48e74065e10c11dd48a6ae88ac002a7515000000001976a9148494fdf4a8c4f6b8df1a245f2863a3c9d670ef1b88ac00452c16000000001976a914f1f73a3c6f3024336b39cfbfc830b01dbbde02ca88ac0084d717000000001976a914d94d81f3999836c74e4207e3f47e23ce9c8d720188ac00b10819000000001976a9148d5820ce00f543810edbf7f0de1a39ce9eabfd9488ac0065cd1d000000001976a9148c68632b72ccd72f09d05d13ff0a54350a2ab0e488ac00000000'
