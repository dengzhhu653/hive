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

final class ScheduledQueryMaintenanceRequestType
{
    const CREATE = 1;

    const ALTER = 2;

    const DROP = 3;

    static public $__names = array(
        1 => 'CREATE',
        2 => 'ALTER',
        3 => 'DROP',
    );
}

