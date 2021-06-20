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

class ReplLastIdInfo
{
    static public $isValidate = false;

    static public $_TSPEC = array(
        1 => array(
            'var' => 'database',
            'isRequired' => true,
            'type' => TType::STRING,
        ),
        2 => array(
            'var' => 'lastReplId',
            'isRequired' => true,
            'type' => TType::I64,
        ),
        3 => array(
            'var' => 'table',
            'isRequired' => false,
            'type' => TType::STRING,
        ),
        4 => array(
            'var' => 'catalog',
            'isRequired' => false,
            'type' => TType::STRING,
        ),
        5 => array(
            'var' => 'partitionList',
            'isRequired' => false,
            'type' => TType::LST,
            'etype' => TType::STRING,
            'elem' => array(
                'type' => TType::STRING,
                ),
        ),
    );

    /**
     * @var string
     */
    public $database = null;
    /**
     * @var int
     */
    public $lastReplId = null;
    /**
     * @var string
     */
    public $table = null;
    /**
     * @var string
     */
    public $catalog = null;
    /**
     * @var string[]
     */
    public $partitionList = null;

    public function __construct($vals = null)
    {
        if (is_array($vals)) {
            if (isset($vals['database'])) {
                $this->database = $vals['database'];
            }
            if (isset($vals['lastReplId'])) {
                $this->lastReplId = $vals['lastReplId'];
            }
            if (isset($vals['table'])) {
                $this->table = $vals['table'];
            }
            if (isset($vals['catalog'])) {
                $this->catalog = $vals['catalog'];
            }
            if (isset($vals['partitionList'])) {
                $this->partitionList = $vals['partitionList'];
            }
        }
    }

    public function getName()
    {
        return 'ReplLastIdInfo';
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
                        $xfer += $input->readString($this->database);
                    } else {
                        $xfer += $input->skip($ftype);
                    }
                    break;
                case 2:
                    if ($ftype == TType::I64) {
                        $xfer += $input->readI64($this->lastReplId);
                    } else {
                        $xfer += $input->skip($ftype);
                    }
                    break;
                case 3:
                    if ($ftype == TType::STRING) {
                        $xfer += $input->readString($this->table);
                    } else {
                        $xfer += $input->skip($ftype);
                    }
                    break;
                case 4:
                    if ($ftype == TType::STRING) {
                        $xfer += $input->readString($this->catalog);
                    } else {
                        $xfer += $input->skip($ftype);
                    }
                    break;
                case 5:
                    if ($ftype == TType::LST) {
                        $this->partitionList = array();
                        $_size631 = 0;
                        $_etype634 = 0;
                        $xfer += $input->readListBegin($_etype634, $_size631);
                        for ($_i635 = 0; $_i635 < $_size631; ++$_i635) {
                            $elem636 = null;
                            $xfer += $input->readString($elem636);
                            $this->partitionList []= $elem636;
                        }
                        $xfer += $input->readListEnd();
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
        $xfer += $output->writeStructBegin('ReplLastIdInfo');
        if ($this->database !== null) {
            $xfer += $output->writeFieldBegin('database', TType::STRING, 1);
            $xfer += $output->writeString($this->database);
            $xfer += $output->writeFieldEnd();
        }
        if ($this->lastReplId !== null) {
            $xfer += $output->writeFieldBegin('lastReplId', TType::I64, 2);
            $xfer += $output->writeI64($this->lastReplId);
            $xfer += $output->writeFieldEnd();
        }
        if ($this->table !== null) {
            $xfer += $output->writeFieldBegin('table', TType::STRING, 3);
            $xfer += $output->writeString($this->table);
            $xfer += $output->writeFieldEnd();
        }
        if ($this->catalog !== null) {
            $xfer += $output->writeFieldBegin('catalog', TType::STRING, 4);
            $xfer += $output->writeString($this->catalog);
            $xfer += $output->writeFieldEnd();
        }
        if ($this->partitionList !== null) {
            if (!is_array($this->partitionList)) {
                throw new TProtocolException('Bad type in structure.', TProtocolException::INVALID_DATA);
            }
            $xfer += $output->writeFieldBegin('partitionList', TType::LST, 5);
            $output->writeListBegin(TType::STRING, count($this->partitionList));
            foreach ($this->partitionList as $iter637) {
                $xfer += $output->writeString($iter637);
            }
            $output->writeListEnd();
            $xfer += $output->writeFieldEnd();
        }
        $xfer += $output->writeFieldStop();
        $xfer += $output->writeStructEnd();
        return $xfer;
    }
}
