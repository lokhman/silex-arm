<?php

namespace Lokhman\Silex\ARM;

use Symfony\Component\HttpFoundation\File\File;
use Symfony\Component\HttpFoundation\File\UploadedFile;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Types\Type;

/**
 * Entity metadata class.
 *
 * @final
 *
 * @author Alexander Lokhman <alex.lokhman@gmail.com>
 * @link https://github.com/lokhman/silex-arm
 */
final class Metadata {

    const EXCEPTION = 'Unable to change locked metadata.';

    private $_locked;

    /** @var \Doctrine\DBAL\Platforms\AbstractPlatform */
    private $platform;
    private $updir;

    private $schema = [];
    private $primary;
    private $required = [];
    private $trans = [];
    private $files = [];
    private $groups = [];
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
     * Set file upload directory.
     *
     * @param string $updir
     *
     * @throws \RuntimeException
     * @return \Lokhman\Silex\ARM\Metadata
     */
    public function setUpdir($updir) {
        if ($this->_locked) {
            throw new \RuntimeException(self::EXCEPTION);
        }
        if (!is_dir($updir) && !mkdir($updir, 0777, true)) {
            throw new \RuntimeException('Unable to create upload folder.');
        }
        $this->updir = $updir;
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
     * Convert file object to path.
     *
     * @param type $file
     *
     * @return string
     */
    private function fileToPath($file) {
        if ($file instanceof UploadedFile) {
            $name = base_convert(uniqid(dechex(mt_rand(0, 1e3))), 16, 36);
            if ('' !== $extension = $file->getClientOriginalExtension()) {
                $name .= '.' . mb_strtolower($extension);
            }
            return $file->move($this->updir, $name)->getRealPath();
        } elseif ($file instanceof \SplFileInfo) {
            return $file->getPathname();
        } else {
            return $file;
        }
    }

    /**
     * Convert path to file object.
     *
     * @param string $path
     *
     * @return Symfony\Component\HttpFoundation\File\File
     */
    private function pathToFile($path) {
        return new File($path, false);
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

        // if column is file
        if ($value && $this->isFile($column)) {
            if ($this->schema[$column] === Type::SIMPLE_ARRAY) {
                $value = array_map([$this, 'fileToPath'], $value);
            } else {
                $value = $this->fileToPath($value);
            }
        }

        // convert to database value
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

        // convert to PHP value
        $type = Type::getType($this->schema[$column]);
        $value = $type->convertToPHPValue($value, $this->platform);

        // if column is file
        if ($value && $this->isFile($column)) {
            if ($this->schema[$column] === Type::SIMPLE_ARRAY) {
                // filter is required to remove "" values from database
                return array_map([$this, 'pathToFile'], array_filter($value));
            } else {
                return $this->pathToFile($value);
            }
        }

        return $value;
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
        $this->files[] = $column;
        return $this;
    }

    /**
     * Get array of file column names.
     *
     * @return array
     */
    public function getFiles() {
        return $this->files;
    }

    /**
     * Check if column is file.
     *
     * @param string $column
     *
     * @return boolean
     */
    public function isFile($column) {
        return in_array($column, $this->files);
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
        $this->groups[] = $column;
        return $this;
    }

    /**
     * Get array of group column names.
     *
     * @return array
     */
    public function getGroups() {
        return $this->groups;
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
