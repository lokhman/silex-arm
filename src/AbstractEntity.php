<?php

namespace Lokhman\Silex\ARM;

use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Symfony\Component\HttpFoundation\File\File;
use Lokhman\Silex\ARM\Exception\EntityException;

/**
 * Abstract entity class.
 *
 * @abstract
 *
 * @author Alexander Lokhman <alex.lokhman@gmail.com>
 * @link https://github.com/lokhman/silex-arm
 */
abstract class AbstractEntity implements \ArrayAccess, \IteratorAggregate, \Serializable {

    const PRIMARY = 'primary';
    const REQUIRED = 'required';
    const TRANS = 'trans';
    const FILE = 'file';
    const GROUP = 'group';
    const POSITION = 'position';

    /**
     * @static
     * @var array
     */
    private static $metadata;

    /** @var array */
    private $data;

    /**
     * Overriddable static method that returns repository class.
     *
     * @static
     *
     * @return string
     */
    public static function repository() {
        return 'Lokhman\Silex\ARM\Repository';
    }

    /**
     * Overriddable static method that returns entity schema.
     *
     * @static
     *
     * @return array
     */
    protected abstract static function schema();

    /**
     * Throw EntityException with class prefixed.
     *
     * @final
     * @static
     *
     * @param string $message
     * @param mixed  $code     [optional]
     * @param mixed  $previous [optional]
     *
     * @throws \Lokhman\Silex\ARM\Exception\EntityException
     * @return void
     */
    public static final function raise($message, $code = null, $previous = null) {
        throw new EntityException(static::class . ': ' . $message, $code, $previous);
    }

    /**
     * Get entity metadata.
     *
     * @final
     * @static
     *
     * @return \Lokhman\Silex\ARM\Metadata
     */
    public static final function metadata() {
        return self::$metadata[static::class];
    }

    /**
     * Static method for extracting raw data from entity.
     *
     * @final
     * @static
     *
     * @param \Lokhman\Silex\ARM\AbstractEntity $entity
     *
     * @return array
     */
    public static final function raw(AbstractEntity $entity) {
        $data = null;
        foreach ($entity as $key => $value) {
            if (!$value instanceof Entity) {
                $data[$key] = $value;
            }
        }
        return $data;
    }

    /**
     * Static method for generating metadata array for current entity.
     *
     * @final
     * @static
     *
     * @param \Doctrine\DBAL\Platforms\AbstractPlatform $platform
     *
     * @throws \Lokhman\Silex\ARM\Exception\EntityException
     * @return void
     */
    public static final function init(AbstractPlatform $platform) {
        if (isset(self::$metadata[static::class])) {
            self::raise('Entity class was already initialised.');
        }

        $metadata = new Metadata();
        $metadata->setPlatform($platform);
        foreach (static::schema() as $column => $types) {
            if ('' == $column = trim($column)) {
                self::raise('Column name cannot be empty.');
            }
            if (!is_string($types) || '' == $types = trim($types)) {
                self::raise('Schema type should be a valid string.');
            }
            $types = preg_split('/\s+/', $types);
            foreach (array_unique($types) as $type) {
                if ($type == self::PRIMARY) {
                    if ($metadata->hasPrimary()) {
                        self::raise('Multiple primary keys defined.');
                    }
                    $metadata->setPrimary($column);
                } elseif ($type == self::REQUIRED) {
                    $metadata->addRequired($column);
                } elseif ($type == self::TRANS) {
                    $metadata->addTrans($column);
                } elseif ($type == self::FILE) {
                    // file is always STRING
                    $metadata->addSchema($column, Type::STRING);
                    $metadata->addFile($column);
                } elseif ($type == self::GROUP) {
                    $metadata->addGroup($column);
                } elseif ($type == self::POSITION) {
                    if ($metadata->hasPosition()) {
                        self::raise('Multiple position columns defined.');
                    }
                    if (in_array(self::REQUIRED, $types)) {
                        self::raise('Position column cannot be required.');
                    }
                    if (in_array(self::TRANS, $types)) {
                        self::raise('Position column cannot be translatable.');
                    }
                    if (in_array(self::GROUP, $types)) {
                        self::raise('Position column cannot be used as group.');
                    }
                    // position is always INTEGER
                    $metadata->addSchema($column, Type::INTEGER);
                    $metadata->setPosition($column);
                } elseif (Type::hasType($type)) {
                    if ($metadata->hasSchema($column)) {
                        self::raise('Column "' . $column . '" already has type.');
                    }
                    $metadata->addSchema($column, $type);
                } else {
                    self::raise('Unknown type "' . $type . '" in schema.');
                }
            }
            if (!$metadata->hasSchema($column)) {
                self::raise('Column "' . $column . '" must have type defined.');
            }
        }

        if (!$metadata->hasPrimary()) {
            self::raise('Entity must have a primary column.');
        }

        self::$metadata[static::class] = $metadata->lock();
    }

    /**
     * Entity constructor.
     *
     * @param array $data [optional]
     */
    public function __construct(array $data = null) {
        $this->data = $data ? : [];
    }

    /**
     * Check if entity property exists.
     *
     * @final
     *
     * @param string $offset
     *
     * @return bool
     */
    public final function offsetExists($offset) {
        return isset($this->data[$offset]);
    }

    /**
     * Get entity property converted to schema type.
     *
     * @final
     *
     * @param string $offset
     *
     * @throws \Lokhman\Silex\ARM\Exception\EntityException
     * @return mixed
     */
    public final function offsetGet($offset) {
        if (!array_key_exists($offset, $this->data)) {
            self::raise('Undefined column "' . $offset . '".');
        }
        $metadata = self::$metadata[static::class];
        if ($metadata->isFile($offset)) {
            return new File($this->data[$offset]);
        }
        return $metadata->getSchemaPhpValue($offset, $this->data[$offset]);
    }

    /**
     * Set entity property to database type.
     *
     * @final
     *
     * @param string    $offset
     * @param mixed     $value
     *
     * @throws \Lokhman\Silex\ARM\Exception\EntityException
     * @return void
     */
    public final function offsetSet($offset, $value) {
        if ($offset === null) {
            self::raise('Entity must have a column defined.');
        }
        $metadata = self::$metadata[static::class];
        if ($value instanceof File && $metadata->isFile($offset)) {
            $value = $value->getPathname();
        } else {
            $value = $metadata->getSchemaDatabaseValue($offset, $value);
        }
        $this->data[$offset] = $value;
    }

    /**
     * Unset entity property.
     *
     * @final
     *
     * @param string $offset
     *
     * @return void
     */
    public final function offsetUnset($offset) {
        unset($this->data[$offset]);
    }

    /**
     * Generator for entity properties.
     *
     * @final
     *
     * @return \Iterator
     */
    public final function getIterator() {
        foreach ($this->data as $key => $value) {
            yield $key => $value;
        }
    }

    /**
     * Entity serializer.
     *
     * @final
     *
     * @return string
     */
    public final function serialize() {
        return serialize($this->data);
    }

    /**
     * Entity unserializer.
     *
     * @final
     *
     * @param string $serialized
     *
     * @return void
     */
    public final function unserialize($serialized) {
        $this->data = unserialize($serialized);
    }

}
