<?php

namespace Lokhman\Silex\ARM;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Types\Type;

/**
 * Metadata class.
 *
 * @final
 */
final class Metadata {

    const EXCEPTION = 'Unable to change locked metadata.';

    private $_locked;

    /** @var \Doctrine\DBAL\Platforms\AbstractPlatform */
    private $platform;
    private $schema = [];
    private $primary;
    private $required = [];
    private $trans = [];
    private $file = [];
    private $group = [];
    private $position;

    /**
     * Lock metadata.
     *
     * @return \Lokhman\Silex\ARM\Metadata
     */
    public function lock() {
        $this->_locked = true;
        return $this;
    }

    /**
     * Set DBAL platform.
     *
     * @param \Doctrine\DBAL\Platforms\AbstractPlatform $platform
     *
     * @throws \RuntimeException
     * @return \Lokhman\Silex\ARM\Metadata
     */
    public function setPlatform(AbstractPlatform $platform) {
        if ($this->_locked) {
            throw new \RuntimeException(self::EXCEPTION);
        }
        $this->platform = $platform;
        return $this;
    }

    /**
     * Add column and type to schema.
     *
     * @param string $column
     * @param string $type
     *
     * @throws \RuntimeException
     * @return \Lokhman\Silex\ARM\Metadata
     */
    public function addSchema($column, $type) {
        if ($this->_locked) {
            throw new \RuntimeException(self::EXCEPTION);
        }
        $this->schema[$column] = $type;
        return $this;
    }

    /**
     * Check if column exists in schema.
     *
     * @param string $column
     *
     * @return boolean
     */
    public function hasSchema($column) {
        return isset($this->schema[$column]);
    }

    /**
     * Get array of column names.
     *
     * @return array
     */
    public function getColumns() {
        return array_keys($this->schema);
    }

    /**
     * Get schema database value.
     *
     * @param string $column
     * @param mixed  $value
     *
     * @return mixed
     */
    public function getSchemaDatabaseValue($column, $value) {
        if (!isset($this->schema[$column])) {
            return $value;
        }
        $type = Type::getType($this->schema[$column]);
        return $type->convertToDatabaseValue($value, $this->platform);
    }

    /**
     * Get schema PHP value.
     *
     * @param string $column
     * @param mixed  $value
     *
     * @return mixed
     */
    public function getSchemaPhpValue($column, $value) {
        if (!isset($this->schema[$column])) {
            return $value;
        }
        $type = Type::getType($this->schema[$column]);
        return $type->convertToPHPValue($value, $this->platform);
    }

    /**
     * Set primary key column.
     *
     * @param string $column
     *
     * @throws \RuntimeException
     * @return \Lokhman\Silex\ARM\Metadata
     */
    public function setPrimary($column) {
        if ($this->_locked) {
            throw new \RuntimeException(self::EXCEPTION);
        }
        $this->primary = $column;
        return $this;
    }

    /**
     * Get primary key column name.
     *
     * @return string|null
     */
    public function getPrimary() {
        return $this->primary;
    }

    /**
     * Check if schema has primary key column.
     *
     * @return boolean
     */
    public function hasPrimary() {
        return $this->primary !== null;
    }

    /**
     * Add required column.
     *
     * @param string $column
     *
     * @throws \RuntimeException
     * @return \Lokhman\Silex\ARM\Metadata
     */
    public function addRequired($column) {
        if ($this->_locked) {
            throw new \RuntimeException(self::EXCEPTION);
        }
        $this->required[] = $column;
        return $this;
    }

    /**
     * Get array of required column names.
     *
     * @return array
     */
    public function getRequired() {
        return $this->required;
    }

    /**
     * Check if column is required.
     *
     * @param string $column
     *
     * @return boolean
     */
    public function isRequired($column) {
        return in_array($column, $this->required);
    }

    /**
     * Add translatable column.
     *
     * @param string $column
     *
     * @throws \RuntimeException
     * @return \Lokhman\Silex\ARM\Metadata
     */
    public function addTrans($column) {
        if ($this->_locked) {
            throw new \RuntimeException(self::EXCEPTION);
        }
        $this->trans[] = $column;
        return $this;
    }

    /**
     * Get array of translatable column names
     *
     * @return array
     */
    public function getTrans() {
        return $this->trans;
    }

    /**
     * Check if column is translatable.
     *
     * @param string $column
     *
     * @return boolean
     */
    public function isTrans($column) {
        return in_array($column, $this->trans);
    }

    /**
     * Add file column.
     *
     * @param string $column
     *
     * @throws \RuntimeException
     * @return \Lokhman\Silex\ARM\Metadata
     */
    public function addFile($column) {
        if ($this->_locked) {
            throw new \RuntimeException(self::EXCEPTION);
        }
        $this->file[] = $column;
        return $this;
    }

    /**
     * Add group column.
     *
     * @param string $column
     *
     * @throws \RuntimeException
     * @return \Lokhman\Silex\ARM\Metadata
     */
    public function addGroup($column) {
        if ($this->_locked) {
            throw new \RuntimeException(self::EXCEPTION);
        }
        $this->group[] = $column;
        return $this;
    }

    /**
     * Get array of group column names.
     *
     * @return array
     */
    public function getGroup() {
        return $this->group;
    }

    /**
     * Set position column.
     *
     * @param string $column
     *
     * @throws \RuntimeException
     * @return \Lokhman\Silex\ARM\Metadata
     */
    public function setPosition($column) {
        if ($this->_locked) {
            throw new \RuntimeException(self::EXCEPTION);
        }
        $this->position = $column;
        return $this;
    }

    /**
     * Get position column name.
     *
     * @return string|null
     */
    public function getPosition() {
        return $this->position;
    }

    /**
     * Check if schema has position column.
     *
     * @return boolean
     */
    public function hasPosition() {
        return $this->position !== null;
    }

}
