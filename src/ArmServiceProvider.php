<?php

namespace Lokhman\Silex\ARM;

use Silex\ServiceProviderInterface;
use Silex\Application;

/**
 * Simple and lightweight Array-like Relational Mapping library for Silex micro-framework.
 *
 * @author Alexander Lokhman <alex.lokhman@gmail.com>
 * @link https://github.com/lokhman/silex-arm
 */
class ArmServiceProvider implements ServiceProviderInterface {

    /**
     * Registers ARM service in the given app.
     *
     * @param \Silex\Application $app
     */
    public function register(Application $app) {
        $app['arm'] = $app->share(function($app) {
            // initialise DBAL configuration
            $app['dbs.options.initializer']();

            // check dependencies
            if (!isset($app['locale_fallbacks'])) {
                throw new \RuntimeException('Option "locale_fallbacks" is not defined.');
            }

            // default locale
            $app['arm.locale'] = $app['locale_fallbacks'][0];

            // trans table name
            if (!isset($app['arm.trans'])) {
                $app['arm.trans'] = '_arm_trans';
            }

            // normalise namespace
            if (isset($app['arm.namespace'])) {
                $namespace = trim($app['arm.namespace'], '\\');
                if (strpos($namespace, '\\') === false) {
                    $namespace .= '\\ARM\\Entity';
                }
                $app['arm.namespace'] = $namespace;
            }

            $arm = new \Pimple();
            foreach ($app['arm.mapping'] as $table => $entity) {
                $profile = $app['dbs.default'];
                if (false !== $pos = strpos($table, '.')) {
                    $profile = substr($table, 0, $pos);
                    $table = substr($table, $pos + 1);
                }
                if (!isset($app['dbs'][$profile]) || !$db = $app['dbs'][$profile]) {
                    throw new \RuntimeException('DBAL profile "' . $profile . '" is not defined.');
                }

                // get entity class
                $entity = trim($entity, '\\');
                if (strpos($entity, '\\') === false) {
                    if (!isset($app['arm.namespace'])) {
                        throw new \RuntimeException('Option "arm.namespace" is not defined.');
                    }
                    $entity = $app['arm.namespace'] . '\\' . $entity;
                }
                if (!class_exists($entity)) {
                    throw new \RuntimeException('Class "' . $entity . '" does not exist.');
                }

                // get repository class
                $repository = trim($entity::repository(), '\\');
                if (strpos($repository, '\\') === false) {
                    if (!isset($app['arm.namespace'])) {
                        throw new \RuntimeException('Option "arm.namespace" is not defined.');
                    }
                    $pos = strrpos($app['arm.namespace'], '\\');
                    $namespace = substr($app['arm.namespace'], 0, $pos);
                    $repository = $namespace . '\\Repository\\' . $repository;
                }
                if (!class_exists($repository)) {
                    throw new \RuntimeException('Class "' . $repository . '" does not exist.');
                }

                // initialise repository metadata
                $entity::init($db->getDatabasePlatform());

                // repository generator
                $key = $profile === $app['dbs.default'] ? $table : $profile . '.' . $table;
                $arm[$key] = $app->share(function() use ($app, $repository, $profile, $table, $entity) {
                    return new $repository($app, $profile, $table, $entity);
                });
            }
            return $arm;
        });
    }

    /**
     * Bootstraps the application.
     *
     * @param \Silex\Application $app
     */
    public function boot(Application $app) {
        // ['arm']['table'] -> ['arm:table']
        foreach ($app['arm']->keys() as $table) {
            $app['arm:' . $table] = $app['arm']->raw($table);
        }
    }

}
