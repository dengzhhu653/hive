<?php
namespace metastore;

/**
 * Autogenerated by Thrift Compiler (0.13.0)
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

class ThriftHiveMetastore_get_delegation_token_result
{
    static public $isValidate = false;

    static public $_TSPEC = array(
        0 => array(
            'var' => 'success',
            'isRequired' => false,
            'type' => TType::STRING,
        ),
        1 => array(
            'var' => 'o1',
            'isRequired' => false,
            'type' => TType::STRUCT,
            'class' => '\metastore\MetaException',
        ),
    );

    /**
     * @var string
     */
    public $success = null;
    /**
     * @var \metastore\MetaException
     */
    public $o1 = null;

    public function __construct($vals = null)
    {
        if (is_array($vals)) {
            if (isset($vals['success'])) {
                $this->success = $vals['success'];
            }
            if (isset($vals['o1'])) {
                $this->o1 = $vals['o1'];
            }
        }
    }

    public function getName()
    {
        return 'ThriftHiveMetastore_get_delegation_token_result';
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
                case 0:
                    if ($ftype == TType::STRING) {
                        $xfer += $input->readString($this->success);
                    } else {
                        $xfer += $input->skip($ftype);
                    }
                    break;
                case 1:
                    if ($ftype == TType::STRUCT) {
                        $this->o1 = new \metastore\MetaException();
                        $xfer += $this->o1->read($input);
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
        $xfer += $output->writeStructBegin('ThriftHiveMetastore_get_delegation_token_result');
        if ($this->success !== null) {
            $xfer += $output->writeFieldBegin('success', TType::STRING, 0);
            $xfer += $output->writeString($this->success);
            $xfer += $output->writeFieldEnd();
        }
        if ($this->o1 !== null) {
            $xfer += $output->writeFieldBegin('o1', TType::STRUCT, 1);
            $xfer += $this->o1->write($output);
            $xfer += $output->writeFieldEnd();
        }
        $xfer += $output->writeFieldStop();
        $xfer += $output->writeStructEnd();
        return $xfer;
    }
}
