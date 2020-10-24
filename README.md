# Dynamo2csv
Dynamo2csv

Python 3.X版 Dynamo Database to CSVファイル

機能概要
・指定されたDynamoテーブル情報をCSV出力します。
・ネスト型の構成には対応していません。
・指定されたフォルダ配下に日時のフォルダを作成しテーブル単位でCSVファイルを出力します


利用ライブラリ：
boto3

条件
1. AWS Cliがインストールされており自分のAWSアクセスが設定されていること
   https://docs.aws.amazon.com/ja_jp/cli/latest/userguide/cli-chap-install.html

2. ログイン先のAWWSにDynamodbが作成されていること


実行例：python PyDynamo2csv.py c:\\temp\\test table1,table2


