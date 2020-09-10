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

class AggrStats
{
    static public $isValidate = false;

    static public $_TSPEC = array(
        1 => array(
            'var' => 'colStats',
            'isRequired' => true,
            'type' => TType::LST,
            'etype' => TType::STRUCT,
            'elem' => array(
                'type' => TType::STRUCT,
                'class' => '\metastore\ColumnStatisticsObj',
                ),
        ),
        2 => array(
            'var' => 'partsFound',
            'isRequired' => true,
            'type' => TType::I64,
        ),
        3 => array(
            'var' => 'isStatsCompliant',
            'isRequired' => false,
            'type' => TType::BOOL,
        ),
    );

    /**
     * @var \metastore\ColumnStatisticsObj[]
     */
    public $colStats = null;
    /**
     * @var int
     */
    public $partsFound = null;
    /**
     * @var bool
     */
    public $isStatsCompliant = null;

    public function __construct($vals = null)
    {
        if (is_array($vals)) {
            if (isset($vals['colStats'])) {
                $this->colStats = $vals['colStats'];
            }
            if (isset($vals['partsFound'])) {
                $this->partsFound = $vals['partsFound'];
            }
            if (isset($vals['isStatsCompliant'])) {
                $this->isStatsCompliant = $vals['isStatsCompliant'];
            }
        }
    }

    public function getName()
    {
        return 'AggrStats';
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
                    if ($ftype == TType::LST) {
                        $this->colStats = array();
                        $_size273 = 0;
                        $_etype276 = 0;
                        $xfer += $input->readListBegin($_etype276, $_size273);
                        for ($_i277 = 0; $_i277 < $_size273; ++$_i277) {
                            $elem278 = null;
                            $elem278 = new \metastore\ColumnStatisticsObj();
                            $xfer += $elem278->read($input);
                            $this->colStats []= $elem278;
                        }
                        $xfer += $input->readListEnd();
                    } else {
                        $xfer += $input->skip($ftype);
                    }
                    break;
                case 2:
                    if ($ftype == TType::I64) {
                        $xfer += $input->readI64($this->partsFound);
                    } else {
                        $xfer += $input->skip($ftype);
                    }
                    break;
                case 3:
                    if ($ftype == TType::BOOL) {
                        $xfer += $input->readBool($this->isStatsCompliant);
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
        $xfer += $output->writeStructBegin('AggrStats');
        if ($this->colStats !== null) {
            if (!is_array($this->colStats)) {
                throw new TProtocolException('Bad type in structure.', TProtocolException::INVALID_DATA);
            }
            $xfer += $output->writeFieldBegin('colStats', TType::LST, 1);
            $output->writeListBegin(TType::STRUCT, count($this->colStats));
            foreach ($this->colStats as $iter279) {
                $xfer += $iter279->write($output);
            }
            $output->writeListEnd();
            $xfer += $output->writeFieldEnd();
        }
        if ($this->partsFound !== null) {
            $xfer += $output->writeFieldBegin('partsFound', TType::I64, 2);
            $xfer += $output->writeI64($this->partsFound);
            $xfer += $output->writeFieldEnd();
        }
        if ($this->isStatsCompliant !== null) {
            $xfer += $output->writeFieldBegin('isStatsCompliant', TType::BOOL, 3);
            $xfer += $output->writeBool($this->isStatsCompliant);
            $xfer += $output->writeFieldEnd();
        }
        $xfer += $output->writeFieldStop();
        $xfer += $output->writeStructEnd();
        return $xfer;
    }
}
