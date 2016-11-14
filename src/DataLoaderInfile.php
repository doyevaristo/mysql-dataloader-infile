<?php 
namespace Doyevaristo;

use Nette\Database\Connection;
use Nette\Database\Context;

class DataLoaderInfile{

    const DS = DIRECTORY_SEPARATOR;

    protected $dbConnection;
    protected $tableName;
    protected $tempTableName;
    protected $prefixTempTablename = 'temp_';
    protected $databaseName;
    protected $dataDir;
    protected $dataDirWithDbName;
    protected $csvfile;
    protected $columns;
    protected $skipRows = 1;
    protected $dataDirFile; //ready to load data in file 

    public function __construct($user,$pass,$host,$databaseName){
        $this->setDatabase($databaseName);
        $this->setupDatabaseConnection($user,$pass,$host,$databaseName);
        $this->configureDataDir();
        $this->configureDataDirWithDbName();
    }

    public function run(){
    
        //create temporary table
        $this->createTemporyTable();
        //check if columns is set. if no set, set it to default tables columns
        if(!$this->columns){
            $this->setTableColumns();
        }

        $this->getDataDirFile();
        $this->loadDataInfile();
        $this->insertOnDuplicateKey();
        $this->deleteDataFile();
    }


    private function loadDataInfile(){
        $filename = basename($this->getDataDirFile());

        $filename = $this->getDataDirFile();

        $strHeaders = implode(',',$this->columns);
        $query = "LOAD DATA INFILE '{$filename}' 
                INTO TABLE {$this->tempTableName} 
                FIELDS TERMINATED BY ',' 
                ENCLOSED BY '\"'
                LINES TERMINATED BY '\n'
                IGNORE {$this->skipRows} LINES
                ({$strHeaders})";
        $this->dbConnection->query($query);
        return $this;
    }

    private function insertOnDuplicateKey(){
        $tblAlias = 'tbl';
        $parseStrTblColumns = implode(',',$this->columns);

        foreach($this->columns as $f){  $arrayTblColumns[]=$f.'='.$tblAlias.'.'.$f; } //build query part in array: [fieldname]=[temporary_table_name].[fieldname]
        $parseArrayTblColumns = implode(',',$arrayTblColumns);

        $query = "INSERT INTO {$this->tableName} ({$parseStrTblColumns}) 
        (SELECT $parseStrTblColumns FROM {$this->tempTableName} {$tblAlias})
        ON DUPLICATE KEY UPDATE {$parseArrayTblColumns}";

        $this->dbConnection->query($query);
        
        return $this;
    }

    public function csvfile($csvfile){
        $this->csvfile = $csvfile;
        copy($this->csvfile ,$this->getDataDirFile());
        return $this;
    }

    public function setDatabase($databaseName){
        $this->databaseName = $databaseName;
    }

    public function skipRow($row){
        $this->skipRows = $row;
    }

    public function setTable($tablename){
        $this->tableName = $tablename;
        $this->setTempTableName();
        return $this;
    }

    /**
    * Set columns to 
    */
    public function setTableColumns($columns=null){



        if($columns){
            $this->columns = $columns;
        }else{
            $this->columns = $this->getTableColumns();
        }

        return $this;
    }

    public function setDataDir($path=null){
        if($path){
            $this->dataDir = $path;
        }
        $this->dataDir = $this->fetchDataDir();
        return $this;
    }

    public function getDataDir(){
        return $this->dataDir;
    }

    public function getDataDirWithDbName(){
        return $this->dataDirWithDbName;
    }

    private function setTempTableName(){
        
        if($this->tempTableName){
            return $this;
        }

        $this->tempTableName = $this->prefixTempTablename.$this->tableName.time();
        return $this;
    }

    public function getDataDirFile(){

        if($this->dataDirFile){
            return $this->dataDirFile;
        }

        $this->dataDirFile = $this->dataDir.$this->databaseName.SELF::DS.$this->tempTableName.".csv";

        return $this->dataDirFile;
    }

    private function deleteDataFile(){
       if(file_exists($this->dataDirFile)){
           unlink($this->dataDirFile);
       }
    }

    private function configureDataDirWithDbName(){
        $this->dataDirWithDbName = $this->dataDir.$this->databaseName.SELF::DS;
        return $this;
    }

    private function configureDataDir(){
        $data=$this->dbConnection->query("show variables where Variable_Name='datadir'")->fetchPairs();
        $this->dataDir = $data['datadir'];
        return $this;
    }

    public function getPrimaryKey(){
        return $this->dbConnection->table($this->tableName)->getPrimary();
    }

    public function getColumns(){
        return $this->columns;
    }

    public function getTableColumns(){
        $data=$this->dbConnection->query("SHOW COLUMNS FROM {$this->tableName}")->fetchPairs();
        return array_keys($data);
    }

    private function createTemporyTable(){
        $this->dbConnection->query("CREATE TEMPORARY TABLE {$this->tempTableName} LIKE {$this->tableName}");
        return $this;
    }

    private function setupDatabaseConnection($user,$pass,$host,$databaseName,$driver='mysql'){
        $dsn = "{$driver}:host={$host};dbname={$databaseName}";
        $connection = new \Nette\Database\Connection($dsn,$user,$pass);
        $cacheMemoryStorage = new \Nette\Caching\Storages\MemoryStorage;
        $structure = new \Nette\Database\Structure($connection, $cacheMemoryStorage);
        $conventions = new \Nette\Database\Conventions\DiscoveredConventions($structure);
        $context = new \Nette\Database\Context($connection, $structure, $conventions, $cacheMemoryStorage);
        $this->dbConnection=$context;
    }
}