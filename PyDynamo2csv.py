'''
 PyDyanmo2csv

 Create:2020.10.24

'''
import sys, os, boto3
from datetime import datetime as dt
from boto3.dynamodb.conditions import Key

class DynamoDao():
    '''
    Dynamo Db アクセスクラス
    
    1オブジェクト1テーブル想定なので
    
    生成時に、テーブル名を指定してオブジェクトを生成する
    
    
    
    Notes
    --------------------------
    dao = DynamoDao(table='DynamoTableName')
    
    '''

    ITEMS = 'Items'
    META_DATA = 'ResponseMetadata'
    
    def __init__(self, **kwargs):
        self.dynamodb =boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(kwargs['table'])
        
    def scan(self):
        '''
        全データ取得メソッド
        
        
        Paramters
        --------------------------
        
        Returns
        --------------------------
        list
            テーブルレコード
        
        '''
        # データ取得
        response = self.table.scan()
        
        #正常終了か確認
        if 200 != response[DynamoDao.META_DATA]['HTTPStatusCode']:
            print( "error" )
            print( response )

        return response[DynamoDao.ITEMS]

class Dict2Csv():
    '''
    Dict情報をCSVへ変換しファイルへ出力するクラス
    
    Dynamoデータを想定しているので、一度全データを検索し
    
    存在する項目を全て抽出してヘッダー情報を出力する
    
    
    '''

    CSV = '.csv'
    CRLF = '\n'
    
    def __init__(self, **kwargs):
        tdatetime = dt.now()
        tstr = tdatetime.strftime('%Y%m%d_%H%M%S')
        self.folder = os.path.join( kwargs['folder'], tstr)
        if os.path.exists( self.folder ) == False:
            #フォルダ生成
            os.makedirs(self.folder)

    def put( self, filename, dynamo_dict ):
        '''
        
        指定されたファイルにCSV出力する
        
        dynamo_dictは、DyanmoDBのdictデータであるが、
        
        実はDynamoDBって列が自由であったりなかったりするから方法は二つ
        
        1.一度、dictをなめて項目名を全て抽出しておかないとダメ？
        2.それとも出力しながらやるか・・・。
        
        CSVの1行目にヘッダー情報出力したいからどっちでやっても結局2回なめる必要性があるから
        1を採用する。ロジック的にもわかりやすいだろうから
        
        Paramters
        --------------------------
        filename : str
            ファイル名
        data : dict
            出力するdict
        '''

        headers = self._get_header( dynamo_dict )
        
        #ファイルオープン
        with open(os.path.join( self.folder, filename + Dict2Csv.CSV ),'w') as fh:
            # ヘッダー出力
            sbuffer = ''
            for header in headers:
                if len(sbuffer) > 0:
                    sbuffer += ','
                sbuffer += header
            fh.write( sbuffer + Dict2Csv.CRLF)

            #データ出力
            all_buf = ''
            for dynamo_record in dynamo_dict:
                #
                #1行分生成
                
                all_buf += self._get_csv_line( headers, dynamo_record ) + Dict2Csv.CRLF
            fh.write(all_buf)

    def _get_csv_line(self, headers, dynamo_record ):
        '''
        1行分のCSVバッファを返す
        '''
        line_buf = ''
        firstcolumn = True
        for column in headers:
            if firstcolumn:
                firstcolumn = False
            else:
                line_buf += ','
            value = dynamo_record.get(column, '')
            #print('{0}:value[{1}]'.format(column,value))
            line_buf += '{0}'.format(value)
        
        return line_buf
        

    def _get_header(self, dynamo_list):
        '''
        dynamo_listのkey情報を全て取得して、CSVで出力すべき全項目を洗い出す。
        
        またそれがCSVのヘッダー情報となる。

        Paramters
        --------------------------
        dynamo_list : list in dict
            Dynamoから取得したレコード情報
        
        Returns
        --------------------------
        list
            ヘッダー項目名が入ったリスト
        '''
        columns = {}
        
        for record_dict in dynamo_list:
            for key in record_dict.keys():
                if not key in columns:
                    columns[key] = ''

        return list(columns.keys())


if __name__ == '__main__':

    print( 'start')
    args = sys.argv
    if len(args) != 3:
        print(args)
        print( "parameter1: 出力先フォルダ" )
        print( "parameter2: tablename 複数の場合はカンマ付き " )
        print( "ex) python PyDynamo2csv.py c:\\temp\\test table1,table2 " )
        exit(1)

    csv = Dict2Csv(folder=args[1])
    
    tables = args[2].split(',')
    
    for table in tables:
        dao = DynamoDao(table=table)
        records = dao.scan()
        csv.put(table, records )
        print( 'talbe:[{0}] output end'.format(table))

    print('end')
    
