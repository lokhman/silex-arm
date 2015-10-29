<?php

namespace Lokhman\Silex\ARM;

use Silex\Application;
use Doctrine\DBAL\Query\QueryBuilder;
use Lokhman\Silex\ARM\AbstractEntity;
use Lokhman\Silex\ARM\Exception\RepositoryException;

/**
 * Overriddable repository class.
 *
 * @author Alexander Lokhman <alex.lokhman@gmail.com>
 * @link https://github.com/lokhman/silex-arm
 */
class Repository {

    const UNIQUE = '_0f72_';  // 6 bytes

    /** @var \Silex\Application */
    private $app;

    /** @var \Doctrine\DBAL\Connection */
    private $db;
    private $prefix;
    private $table;
    private $entity;
    private $locale;
    private $translate;

    /** @var \Lokhman\Silex\ARM\Metadata */
    private $metadata;

    /**
     * Throw RepositoryException with class prefixed.
     *
     * @final
     * @static
     *
     * @param string $message
     * @param mixed  $code     [optional]
     * @param mixed  $previous [optional]
     *
     * @throws \Lokhman\Silex\ARM\Exception\RepositoryException
     * @return void
     */
    public static final function raise($message, $code = null, $previous = null) {
        throw new RepositoryException(static::class . ': ' . $message, $code, $previous);
    }

    /**
     * Repository constructor.
     *
     * @param \Silex\Application $app
     * @param string             $profile
     * @param string             $table
     * @param string             $entity
     */
    public function __construct(Application $app, $profile, $table, $entity) {
        $this->app = $app;
        $this->table = $table;
        $this->entity = $entity;
        $this->locale = $app['locale'];
        $this->db = $app['dbs'][$profile];
        $this->prefix = $profile === $app['dbs.default'] ? '' : $profile . '.';
        $this->translate = $app['locale'] != $app['arm.locale'];
        $this->metadata = $entity::metadata();
    }

    /**
     * Pre-insert event.
     *
     * @category events
     *
     * @param \Lokhman\Silex\ARM\AbstractEntity $entity
     *
     * @return void
     */
    protected function preInsert(AbstractEntity $entity) { }

    /**
     * Post-insert event.
     *
     * @category events
     *
     * @param \Lokhman\Silex\ARM\AbstractEntity $entity
     *
     * @return void
     */
    protected function postInsert(AbstractEntity $entity) { }

    /**
     * Pre-update event.
     *
     * @category events
     *
     * @param \Lokhman\Silex\ARM\AbstractEntity $entity
     *
     * @return void
     */
    protected function preUpdate(AbstractEntity $entity) { }

    /**
     * Post-update event.
     *
     * @category events
     *
     * @param \Lokhman\Silex\ARM\AbstractEntity $entity
     *
     * @return void
     */
    protected function postUpdate(AbstractEntity $entity) { }

    /**
     * Pre-delete event.
     *
     * @category events
     *
     * @param \Lokhman\Silex\ARM\AbstractEntity $entity
     *
     * @return void
     */
    protected function preDelete(AbstractEntity $entity) { }

    /**
     * Post-delete event.
     *
     * @category events
     *
     * @param \Lokhman\Silex\ARM\AbstractEntity $entity
     *
     * @return void
     */
    protected function postDelete(AbstractEntity $entity) { }

    /**
     * Alias for <code>$this->getDatabase()->createQueryBuilder()</code>.
     *
     * @final
     *
     * @return \Doctrine\DBAL\Query\QueryBuilder
     */
    protected final function qb() {
        return $this->db->createQueryBuilder();
    }

    /**
     * Alias for <code>$this->getDatabase()->getExpressionBuilder()</code>.
     *
     * @final
     *
     * @return \Doctrine\DBAL\Query\Expression\ExpressionBuilder
     */
    protected final function expr() {
        return $this->db->getExpressionBuilder();
    }

    /**
     * Get application instance.
     *
     * @final
     *
     * @return \Silex\Application
     */
    public final function getApp() {
        return $this->app;
    }

    /**
     * Get database connection.
     *
     * @final
     *
     * @return \Doctrine\DBAL\Connection
     */
    public final function getDatabase() {
        return $this->db;
    }

    /**
     * Get entity table name.
     *
     * @final
     *
     * @return string
     */
    public final function getTable() {
        return $this->table;
    }

    /**
     * Get entity class name.
     *
     * @final
     *
     * @return string
     */
    public final function getEntity() {
        return $this->entity;
    }

    /**
     * Get entity metadata.
     *
     * @final
     *
     * @return \Lokhman\Silex\ARM\Metadata
     */
    public final function getMetadata() {
        return $this->metadata;
    }

    /**
     * Check if application runs under foreign locale.
     *
     * @final
     *
     * @return boolean
     */
    public final function isTranslate() {
        return (bool) $this->translate;
    }

    /**
     * Assert if $entity is instance of self entity.
     *
     * @param \Lokhman\Silex\ARM\AbstractEntity $entity
     *
     * @throws \Lokhman\Silex\ARM\Exception\RepositoryException
     * @return void
     */
    private function assert(AbstractEntity $entity) {
        if (!$entity instanceof $this->entity) {
            self::raise('Use entity "' . get_class($entity) . '" with own repository.');
        }
    }

    /**
     * Generate select field for a given table and column.
     *
     * @staticvar integer $index
     *
     * @param \Doctrine\DBAL\Query\QueryBuilder $qb
     * @param \Lokhman\Silex\ARM\Repository     $repository
     * @param string                            $alias
     * @param string                            $column
     *
     * @return string
     */
    private function select(QueryBuilder $qb, Repository $repository, $alias, $column) {
        if (!$this->translate || !$repository->getMetadata()->isTrans($column)) {
            return $alias . '.' . $column . ' ' . $repository->getTable() .
                self::UNIQUE . $column;
        }

        static $index = 0;
        $table = $repository->getTable();
        $t = $this->app['arm.trans'] . $index++;
        $qb->leftJoin($alias, $this->app['arm.trans'], $t,
            $t . '._table = ' . $this->expr()->literal($table) . ' AND ' .
            $t . '._key = ' . $alias . '.' . $repository->getMetadata()->getPrimary() . ' AND ' .
            $t . '._column = ' . $this->expr()->literal($column) . ' AND ' .
            $t . '._locale = ' . $this->expr()->literal($this->locale)
        );

        return 'COALESCE(' . $t . '._content, ' . $alias . '.' . $column . ') ' .
            $table . self::UNIQUE . $column;
    }

    /**
     * Format SELECT parts for a given query.
     *
     * @param \Doctrine\DBAL\Query\QueryBuilder $qb
     * @param array                             $tables
     *
     * @return array
     */
    private function formatSelect(QueryBuilder $qb, array $tables) {
        $select = [];
        foreach ($qb->getQueryPart('select') as $part) {
            if (strpos($part, ' ') !== false) {
                goto keep;  // 'x.y xyz'
            } elseif ($part == '*') {  // '*'
                foreach ($tables as $alias => $repository) {
                    $alias = $alias ? : $table = $repository->getTable();
                    foreach ($repository->getMetadata()->getColumns() as $column) {
                        $select[] = $this->select($qb, $repository, $alias, $column);
                    }
                }
            } elseif (false !== $pos = strpos($part, '.')) {  // 'x.y'
                $repository = $tables[$alias = substr($part, 0, $pos)];
                $column = substr($part, $pos + 1);
                $table = $repository->getTable();
                if ($column == '*') {  // 'x.*'
                    foreach ($repository->getMetadata()->getColumns() as $column) {
                        $select[] = $this->select($qb, $repository, $alias, $column);
                    }
                } else {
                    $select[] = $this->select($qb, $repository, $alias, $column);
                }
            } elseif (in_array($part, $this->metadata->getColumns())) {  // 'y'
                $alias = array_search($this, $tables) ? : $this->table;
                $select[] = $this->select($qb, $this, $alias, $part);
            } else {
                keep: $select[] = $part;
            }
        }
        return array_unique($select);
    }

    /**
     * Format SQL query by replacing {x} and {x.y} tokens.
     *
     * @staticvar string $re
     *
     * @param \Doctrine\DBAL\Query\QueryBuilder $qb
     * @param array                             $tables
     *
     * @throws \Lokhman\Silex\ARM\Exception\RepositoryException
     * @return string
     */
    private function formatQuery(QueryBuilder $qb, array $tables) {
        static $re = '/("|\').*?\1(*SKIP)(*FAIL)|\{(?:[^{}]|(?R))*\}/';
        return preg_replace_callback($re, function($matches) use ($tables) {
            if (false !== $pos = strpos($token = substr($matches[0], 1, -1), '.')) {
                if (!isset($tables[$alias = substr($token, 0, $pos)])) {
                    self::raise('No repository registered for "' . $alias . '".');
                }
                return $tables[$alias]->getTable() . self::UNIQUE . substr($token, $pos + 1);
            } elseif (in_array($token, $this->metadata->getColumns())) {
                return $this->table . self::UNIQUE . $token;
            } else {
                return $matches[0];
            }
        }, $qb->getSQL());
    }

    /**
     * Preprocess and execute SELECT query.
     *
     * @param \Doctrine\DBAL\Query\QueryBuilder $qb
     *
     * @throws \Lokhman\Silex\ARM\Exception\RepositoryException
     * @return \Doctrine\DBAL\Driver\Statement
     */
    private function preprocess(QueryBuilder $qb) {
        // for each FROM part in QueryBuilder
        foreach ($qb->getQueryPart('from') as $part) {
            $table = $table = $this->prefix . $part['table'];
            if (!isset($this->app['arm'][$table])) {
                self::raise('No repository registered for "' . $table . '".');
            }
            $tables[$part['alias']] = $this->app['arm'][$table];
        }

        // for each JOIN part in QueryBuilder
        foreach ($qb->getQueryPart('join') as $part) {
            $table = $this->prefix . $part[0]['joinTable'];
            if (!isset($this->app['arm'][$table])) {
                self::raise('No repository registered for "' . $table . '".');
            }
            $tables[$part[0]['joinAlias']] = $this->app['arm'][$table];
        }

        // re-define select columns
        $qb->select($this->formatSelect($qb, $tables));

        // execute pre-formatted SELECT query ( $qb->execute() )
        return $this->db->executeQuery($this->formatQuery($qb, $tables),
            $qb->getParameters(), $qb->getParameterTypes());
    }

    /**
     * Generator for SELECT queries.
     *
     * @param \Doctrine\DBAL\Query\QueryBuilder $qb
     *
     * @return \Iterator
     */
    protected function generator(QueryBuilder $qb) {
        if ($qb->getType() !== QueryBuilder::SELECT) {
            self::raise('Generator should be used only with SELECT queries.');
        }

        $stmt = $this->preprocess($qb);

        while ($row = $stmt->fetch(\PDO::FETCH_ASSOC)) {
            $from = $join = [];
            foreach ($row as $key => $value) {
                // if column was preprocessed
                if (false !== $pos = strpos($key, self::UNIQUE)) {
                    $table = substr($key, 0, $pos);
                    $column = substr($key, $pos + 6);
                    if ($table === $this->table) {
                        $from[$column] = $value;
                    } else {
                        $join[$table][$column] = $value;
                    }
                } else {
                    $from[$key] = $value;
                }
            }

            $class = $this->entity;
            $entity = new $class($from);
            foreach ($join as $table => $data) {
                // every JOIN will be set as a sub-entity
                $class = $this->app['arm'][$this->prefix . $table]->getEntity();
                $entity[$table] = new $class($data);
            }

            yield $entity;
        }
    }

    /**
     * Find entity by primary key value.
     *
     * @param mixed $id
     *
     * @return \Lokhman\Silex\ARM\AbstractEntity|null
     */
    public function find($id) {
        return $this->generator(
            $this->qb()
                ->select($this->metadata->getColumns())
                ->from($this->table)
                ->where($this->expr()->eq(
                    $this->metadata->getPrimary(),
                    $this->expr()->literal($id))
                )
        )->current();
    }

    /**
     * Find multiple entites ordered by `position` if not specified otherwise.
     *
     * @param mixed $expr  [optional]
     * @param array $order [optional]
     *
     * @return \Iterator
     */
    public function findMany($expr = null, array $order = []) {
        $qb = $this->qb()
            ->select($this->metadata->getColumns())
            ->from($this->table)
            ->where($expr ? : 1);

        // default order is by position if specified
        if (!$order && $this->metadata->hasPosition()) {
            $order[$this->metadata->getPosition()] = 'ASC';
        }

        foreach ($order as $column => $dir) {
            $qb->addOrderBy($column, $dir);
        }

        return $this->generator($qb);
    }

    /**
     * Count entites in the table.
     *
     * @param mixed $expr [optional]
     *
     * @return integer
     */
    public function count($expr = null) {
        return (int) $this->qb()
            ->select('COUNT(*)')
            ->from($this->table)
            ->where($expr ? : 1)
            ->execute()
            ->fetchColumn();
    }

    /**
     * Get MAX of column in the table.
     *
     * @param string $column
     * @param mixed  $expr   [optional]
     *
     * @return integer|null
     *         MAX of column or NULL if empty table
     */
    public function max($column, $expr = null) {
        $max = $this->qb()
            ->select('MAX(' . $column . ')')
            ->from($this->table)
            ->where($expr ? : 1)
            ->execute()
            ->fetchColumn();
        return $max === null ? null : (int) $max;
    }

    /**
     * Begin transaction alias.
     *
     * @return void
     */
    public function transaction() {
        $this->db->beginTransaction();
    }

    /**
     * Commit transaction alias.
     *
     * @return void
     */
    public function commit() {
        $this->db->commit();
    }

    /**
     * Safe rollback.
     *
     * @return void
     */
    public function rollback() {
        if ($this->db->isTransactionActive()) {
            $this->db->rollBack();
        }
    }

    /**
     * Persist entity in the database.
     *
     * @param \Lokhman\Silex\ARM\AbstractEntity $entity
     *
     * @return mixed
     */
    public function persist(AbstractEntity $entity) {
        if (isset($entity[$this->metadata->getPrimary()])) {
            return $this->update($entity);
        } else {
            return $this->insert($entity);
        }
    }

    /**
     * Validate entity for INSERT.
     *
     * @param \Lokhman\Silex\ARM\AbstractEntity $entity
     *
     * @throws \Lokhman\Silex\ARM\Exception\RepositoryException
     * @return void
     */
    private function validateInsert(AbstractEntity $entity) {
        foreach ($this->metadata->getRequired() as $column) {
            if (!isset($entity[$column])) {
                $required[] = $column;
            }
        }
        if (isset($required)) {
            $class = get_class($entity);
            $class::raise('Values for columns "' . implode('", "', $required) .
                '" are required for INSERT operation.');
        }
    }

    /**
     * Validate entity for UPDATE (partial data check).
     *
     * @param \Lokhman\Silex\ARM\AbstractEntity $entity
     *
     * @throws \Lokhman\Silex\ARM\Exception\RepositoryException
     * @return void
     */
    private function validateUpdate(AbstractEntity $entity) {
        foreach ($entity as $column => $value) {
            // if value is FILE and empty ([], null, "")
            if (!$value && $this->metadata->isFile($column)) {
                unset($entity[$column]);
            } elseif ($value === null && $this->metadata->isRequired($column)) {
                $required[] = $column;
            }
        }
        if (isset($required)) {
            $class = get_class($entity);
            $class::raise('Values for columns "' . implode('", "', $required) .
                '" are required for UPDATE operation.');
        }
    }

    /**
     * Create group expression.
     *
     * @param \Lokhman\Silex\ARM\AbstractEntity $entity
     *
     * @return \Doctrine\DBAL\Query\Expression\ExpressionBuilder|null
     *         Expression or NULL if no groups defined
     */
    protected function groupExpression(AbstractEntity $entity) {
        if (!$groups = $this->metadata->getGroups()) {
            return null;
        }

        $andX = $this->expr()->andX();
        foreach ($groups as $group) {
            $expr = $this->expr();
            if (null === $value = $entity[$group]) {
                $andX->add($expr->isNull($group));
            } else {
                $andX->add($expr->eq($group, $expr->literal($value)));
            }
        }
        return $andX;
    }

    /**
     * Calculate and set position for INSERT.
     *
     * @param \Lokhman\Silex\ARM\AbstractEntity $entity
     * @param integer|null                      $position [optional]
     *
     * @return void
     */
    private function positionInsert(AbstractEntity $entity, $position = null) {
        $column = $this->metadata->getPosition();
        if ($position === null) {
            $expr = $this->groupExpression($entity);
            $position = $this->max($column, $expr);
            if ($position === null) {
                $position = 0;
            } else {
                $position++;
            }
        }
        $entity[$column] = (int) $position;
    }

    /**
     * Shift positions in the table.
     *
     * @param \Lokhman\Silex\ARM\AbstractEntity $entity
     * @param integer                           $start
     * @param integer|null                      $stop
     * @param integer                           $delta
     *
     * @return void
     */
    private function positionShift(AbstractEntity $entity, $start, $stop, $delta) {
        $column = $this->metadata->getPosition();

        $qb = $this->qb()
            ->update($this->table)
            ->set($column, $column . ($delta < 0 ? '-1' : '+1'))
            ->where($this->groupExpression($entity) ? : 1)
            ->andWhere($column . ' >= :start')
            ->setParameter('start', $start);

        if ($stop > 0) {
            $qb->andWhere($column . ' < :end')
                ->setParameter('end', $stop);
        }

        $qb->execute();
    }

    /**
     * Calculate and set position for UPDATE.
     *
     * @todo Shift limitation on group and position change at once
     *
     * @param \Lokhman\Silex\ARM\AbstractEntity $entity
     *
     * @return void
     */
    private function positionUpdate(AbstractEntity $entity) {
        $column = $this->metadata->getPosition();
        $primary = $this->metadata->getPrimary();

        // fetch old entity instance
        $oldEntity = $this->find($entity[$primary]);
        $oldPosition = $oldEntity[$column];

        // for each group column if exist
        foreach ($this->metadata->getGroups() as $group) {
            if ($oldEntity[$group] !== $entity[$group]) {
                // if any group is changed shift positions for old set
                $this->positionShift($oldEntity, $oldPosition + 1, null, -1);

                // move entity to the end of new set
                $this->positionInsert($entity);
                return;
            }
        }

        // if new position is defined
        if (isset($entity[$column])) {
            $expr = $this->groupExpression($entity);
            $maxPosition = $this->max($column, $expr);
            if ($maxPosition === null) {
                $maxPosition = 0;
            }

            // limit new position in [0, MAX]
            $newPosition = $entity[$column] =
                min(max(0, (int) $entity[$column]), $maxPosition);

            // if nothing to update
            if ($oldPosition == $newPosition) {
                goto clear;
            }

            if ($oldPosition > $newPosition) {
                $this->positionShift($entity, $newPosition, $oldPosition, +1);
            } else {
                $this->positionShift($entity, $oldPosition + 1, $newPosition + 1, -1);
            }
        } else {
            // unset if NULL
            clear: unset($entity[$column]);
        }
    }

    /**
     * Delete all files associated with the entity.
     *
     * @param AbstractEntity $entity
     *
     * @return void
     */
    private function unlink(AbstractEntity $entity) {
        $class = $this->entity;
        foreach ($this->metadata->getFiles() as $column) {
            if (isset($entity[$column])) {
                $class::unlink($entity[$column]);
            }
        }
    }

    /**
     * Insert entity to the database.
     *
     * @param \Lokhman\Silex\ARM\AbstractEntity $entity
     * @param integer|null                      $position [optional]
     *        Set this parameter ONLY if you know what you are doing
     *
     * @throws \Exception
     * @return string
     *         A string representation of the last inserted ID
     */
    public function insert(AbstractEntity $entity, $position = null) {
        $this->assert($entity);

        try {
            $this->db->beginTransaction();

            // pre-insert event
            $this->preInsert($entity);

            // validate entity
            $this->validateInsert($entity);

            // if position column exists
            if ($this->metadata->hasPosition()) {
                $this->positionInsert($entity, $position);
            }

            // get raw data from entity
            $data = AbstractEntity::raw($entity);

            // attempt to insert entity data to database
            $this->db->insert($this->table, $data);
            $id = $this->db->lastInsertId();

            if ($this->translate) {
                // for each translatable column in schema
                foreach ($this->metadata->getTrans() as $column) {
                    if (isset($data[$column])) {
                        // insert translation to translations table
                        $this->db->insert($this->app['arm.trans'], [
                            '_table' => $this->table,
                            '_key' => $id,
                            '_column' => $column,
                            '_locale' => $this->locale,
                            '_content' => $data[$column],
                        ]);
                    }
                }
            }

            $this->db->commit();

            // set primary key value
            $entity[$this->metadata->getPrimary()] = $id;

            // post-insert event
            $this->postInsert($entity);
        } catch (\Exception $ex) {
            $this->unlink($entity);
            $this->rollback();

            throw $ex;
        }
        return $id;
    }

    /**
     * Insert many entities in one go.
     *
     * @param array   $entities
     * @param boolean $ignoreErrors [optional]
     *
     * @throws \Exception
     * @return void
     */
    public function insertMany(array $entities, $ignoreErrors = false) {
        if (!$first = reset($entities)) {
            return;
        }

        $position = 0;
        if ($this->metadata->hasPosition()) {
            // optimising position calculation
            $this->positionInsert($first);
            $column = $this->metadata->getPosition();
            $position = $first[$column];
        }

        if ($ignoreErrors) {
            // simple loop with ignoring errors
            foreach ($entities as $entity) {
                try {
                    $this->insert($entity, $position);

                    // will never be here on Exception
                    $position++;
                } catch (\Exception $ex) {
                    /* continue; */
                }
            }
        } else {
            try {
                // new transaction with rollback on error
                $this->db->beginTransaction();
                foreach ($entities as $entity) {
                    $this->insert($entity, $position++);
                }
                $this->db->commit();
            } catch (\Exception $ex) {
                foreach ($entities as $entity) {
                    $this->unlink($entity);
                }
                $this->rollback();

                throw $ex;
            }
        }
    }

    /**
     * Update entity in the database (partial data is supported).
     *
     * @param \Lokhman\Silex\ARM\AbstractEntity $entity
     *
     * @throws \Exception
     * @return integer
     *         Number of affected rows
     */
    public function update(AbstractEntity $entity) {
        $this->assert($entity);

        // check if primary key is defined and is not NULL
        $primary = $this->metadata->getPrimary();
        if (!isset($entity[$primary])) {
            self::raise('Primary key is required to update the entity.');
        }

        try {
            $this->db->beginTransaction();

            // pre-update event
            $this->preUpdate($entity);

            // validate entity
            $this->validateUpdate($entity);

            // if position column exists
            if ($this->metadata->hasPosition()) {
                $this->positionUpdate($entity);
            }

            // get raw data from entity
            $data = AbstractEntity::raw($entity);

            $id = $data[$primary];
            unset($data[$primary]);

            if ($this->translate) {
                $sql = 'SELECT COUNT(*) FROM ' . $this->app['arm.trans'] .
                    ' WHERE _table = :_table AND _key = :_key AND ' .
                    '_column = :_column AND _locale = :_locale';

                // for each translatable column in schema
                foreach ($this->metadata->getTrans() as $column) {
                    // if no column presented in the data
                    if (!array_key_exists($column, $data)) {
                        continue;
                    }

                    $trans = [
                        '_table' => $this->table,
                        '_key' => $id,
                        '_column' => $column,
                        '_locale' => $this->locale,
                    ];

                    if ($data[$column] === null) {
                        // if column exists but is NULL: delete translation
                        $this->db->delete($this->app['arm.trans'], $trans);
                    } elseif ($this->db->fetchColumn($sql, $trans) > 0) {
                        // if translation exists: update translation
                        $this->db->update($this->app['arm.trans'], [
                            '_content' => $data[$column],
                        ], $trans);
                    } else {
                        // if translation doesn't exist: insert translation
                        $this->db->insert($this->app['arm.trans'], $trans +
                            ['_content' => $data[$column]]);
                    }

                    // remove value from actual data
                    unset($data[$column]);
                }
            }

            if ($data) {
                // update entity data if there is anything to update
                $result = $this->db->update($this->table, $data, [
                    $primary => $id,
                ]);
            }

            $this->db->commit();

            // post-update event
            $this->postUpdate($entity);
        } catch (\Exception $ex) {
            $this->unlink($entity);
            $this->rollback();

            throw $ex;
        }
        return $result;
    }

    /**
     * Delete entity from the database.
     *
     * @param \Lokhman\Silex\ARM\AbstractEntity $entity
     *
     * @throws \Exception
     * @return integer
     *         Number of affected rows
     */
    public function delete(AbstractEntity $entity) {
        $this->assert($entity);

        // check if primary key is defined and is not NULL
        $primary = $this->metadata->getPrimary();
        if (!isset($entity[$primary])) {
            self::raise('Primary key is required to update the entity.');
        }

        try {
            $this->db->beginTransaction();

            // pre-delete event
            $this->preDelete($entity);

            // update position indexes
            if ($this->metadata->hasPosition()) {
                $position = $entity[$this->metadata->getPosition()];
                $this->positionShift($entity, $position + 1, null, -1);
            }

            // delete all translations for entity
            $this->db->delete($this->app['arm.trans'], [
                '_table' => $this->table,
                '_key' => $entity[$primary],
            ]);

            // attempt to delete entity
            $result = $this->db->delete($this->table, [
                $primary => $entity[$primary],
            ]);

            // delete all files
            $this->unlink($entity);

            $this->db->commit();

            // unset primary key
            unset($entity[$primary]);

            // post-delete event
            $this->postDelete($entity);
        } catch (\Exception $ex) {
            $this->rollback();

            throw $ex;
        }
        return $result;
    }

}
