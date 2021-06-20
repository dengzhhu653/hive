<?php
namespace metastore;

/**
 * Autogenerated by Thrift Compiler (0.14.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
use Thrift\Base\TBase;
use Thrift\Type\TType;
use Thrift\Type\TMessageType;
use Thrift\Exception\TException;
use Thrift\Exception\TProtocolException;
use Thrift\Protocol\TProtocol;
use Thrift\Protocol\TBinaryProtocolAccelerated;
use Thrift\Exception\TApplicationException;

class RenamePartitionRequest
{
    static public $isValidate = false;

    static public $_TSPEC = array(
        1 => array(
            'var' => 'catName',
            'isRequired' => false,
            'type' => TType::STRING,
        ),
        2 => array(
            'var' => 'dbName',
            'isRequired' => true,
            'type' => TType::STRING,
        ),
        3 => array(
            'var' => 'tableName',
            'isRequired' => true,
            'type' => TType::STRING,
        ),
        4 => array(
            'var' => 'partVals',
            'isRequired' => true,
            'type' => TType::LST,
            'etype' => TType::STRING,
            'elem' => array(
                'type' => TType::STRING,
                ),
        ),
        5 => array(
            'var' => 'newPart',
            'isRequired' => true,
            'type' => TType::STRUCT,
            'class' => '\metastore\Partition',
        ),
        6 => array(
            'var' => 'validWriteIdList',
            'isRequired' => false,
            'type' => TType::STRING,
        ),
    );

    /**
     * @var string
     */
    public $catName = null;
    /**
     * @var string
     */
    public $dbName = null;
    /**
     * @var string
     */
    public $tableName = null;
    /**
     * @var string[]
     */
    public $partVals = null;
    /**
     * @var \metastore\Partition
     */
    public $newPart = null;
    /**
     * @var string
     */
    public $validWriteIdList = null;

    public function __construct($vals = null)
    {
        if (is_array($vals)) {
            if (isset($vals['catName'])) {
                $this->catName = $vals['catName'];
            }
            if (isset($vals['dbName'])) {
                $this->dbName = $vals['dbName'];
            }
            if (isset($vals['tableName'])) {
                $this->tableName = $vals['tableName'];
            }
            if (isset($vals['partVals'])) {
                $this->partVals = $vals['partVals'];
            }
            if (isset($vals['newPart'])) {
                $this->newPart = $vals['newPart'];
            }
            if (isset($vals['validWriteIdList'])) {
                $this->validWriteIdList = $vals['validWriteIdList'];
            }
        }
    }

    public function getName()
    {
        return 'RenamePartitionRequest';
    }


    public function read($input)
    {
        $xfer = 0;
        $fname = null;
        $ftype = 0;
        $fid = 0;
        $xfer += $input->readStructBegin($fname);
        while (true) {
            $xfer += $input->readFieldBegin($fname, $ftype, $fid);
            if ($ftype == TType::STOP) {
                break;
            }
            switch ($fid) {
                case 1:
                    if ($ftype == TType::STRING) {
                        $xfer += $input->readString($this->catName);
                    } else {
                        $xfer += $input->skip($ftype);
                    }
                    break;
                case 2:
                    if ($ftype == TType::STRING) {
                        $xfer += $input->readString($this->dbName);
                    } else {
                        $xfer += $input->skip($ftype);
                    }
                    break;
                case 3:
                    if ($ftype == TType::STRING) {
                        $xfer += $input->readString($this->tableName);
                    } else {
                        $xfer += $input->skip($ftype);
                    }
                    break;
                case 4:
                    if ($ftype == TType::LST) {
                        $this->partVals = array();
                        $_size1096 = 0;
                        $_etype1099 = 0;
                        $xfer += $input->readListBegin($_etype1099, $_size1096);
                        for ($_i1100 = 0; $_i1100 < $_size1096; ++$_i1100) {
                            $elem1101 = null;
                            $xfer += $input->readString($elem1101);
                            $this->partVals []= $elem1101;
                        }
                        $xfer += $input->readListEnd();
                    } else {
                        $xfer += $input->skip($ftype);
                    }
                    break;
                case 5:
                    if ($ftype == TType::STRUCT) {
                        $this->newPart = new \metastore\Partition();
                        $xfer += $this->newPart->read($input);
                    } else {
                        $xfer += $input->skip($ftype);
                    }
                    break;
                case 6:
                    if ($ftype == TType::STRING) {
                        $xfer += $input->readString($this->validWriteIdList);
                    } else {
                        $xfer += $input->skip($ftype);
                    }
                    break;
                default:
                    $xfer += $input->skip($ftype);
                    break;
            }
            $xfer += $input->readFieldEnd();
        }
        $xfer += $input->readStructEnd();
        return $xfer;
    }

    public function write($output)
    {
        $xfer = 0;
        $xfer += $output->writeStructBegin('RenamePartitionRequest');
        if ($this->catName !== null) {
            $xfer += $output->writeFieldBegin('catName', TType::STRING, 1);
            $xfer += $output->writeString($this->catName);
            $xfer += $output->writeFieldEnd();
        }
        if ($this->dbName !== null) {
            $xfer += $output->writeFieldBegin('dbName', TType::STRING, 2);
            $xfer += $output->writeString($this->dbName);
            $xfer += $output->writeFieldEnd();
        }
        if ($this->tableName !== null) {
            $xfer += $output->writeFieldBegin('tableName', TType::STRING, 3);
            $xfer += $output->writeString($this->tableName);
            $xfer += $output->writeFieldEnd();
        }
        if ($this->partVals !== null) {
            if (!is_array($this->partVals)) {
                throw new TProtocolException('Bad type in structure.', TProtocolException::INVALID_DATA);
            }
            $xfer += $output->writeFieldBegin('partVals', TType::LST, 4);
            $output->writeListBegin(TType::STRING, count($this->partVals));
            foreach ($this->partVals as $iter1102) {
                $xfer += $output->writeString($iter1102);
            }
            $output->writeListEnd();
            $xfer += $output->writeFieldEnd();
        }
        if ($this->newPart !== null) {
            if (!is_object($this->newPart)) {
                throw new TProtocolException('Bad type in structure.', TProtocolException::INVALID_DATA);
            }
            $xfer += $output->writeFieldBegin('newPart', TType::STRUCT, 5);
            $xfer += $this->newPart->write($output);
            $xfer += $output->writeFieldEnd();
        }
        if ($this->validWriteIdList !== null) {
            $xfer += $output->writeFieldBegin('validWriteIdList', TType::STRING, 6);
            $xfer += $output->writeString($this->validWriteIdList);
            $xfer += $output->writeFieldEnd();
        }
        $xfer += $output->writeFieldStop();
        $xfer += $output->writeStructEnd();
        return $xfer;
    }
}
