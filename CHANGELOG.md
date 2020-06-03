# Changelog

## 2.0.1
 * Added start_on field to tasks schema

## 2.0.0
 * Deprecated `id` for `gid` [#16](https://github.com/singer-io/tap-asana/pull/16)

## 1.0.2
 * Added workaround for int ID deprecation [#13](https://github.com/singer-io/tap-asana/pull/13)
   * First, attempts to parse `gid` as `int` and assign it to `id` for no visible effect on the current data
   * This may result in IDs being integer *or* string if Asana changes the value of `gid`

## 1.0.1
 * Added custom_fields and tags to tasks schema
 
## 1.0.0
 * Releasing from Beta --> GA

