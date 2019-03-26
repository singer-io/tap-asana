# tap-asana

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Asana](https://asana.com/developers/)
- Extracts the following resources:
  - [Projects](https://asana.com/developers/api-reference/projects)
  - [Tags](https://asana.com/developers/api-reference/tags)
  - [Tasks](https://asana.com/developers/api-reference/tasks)
  - [Users](https://asana.com/developers/api-reference/users)
  - [Workspaces](https://asana.com/developers/api-reference/workspaces)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick Start

1. Install

```
$ pip install tap-asana
```

2. Create the config file

   Create a JSON file called `config.json`. Its contents should look like:

```json
{
  "client_id": "111",
  "client_secret": "xxx",
  "redirect_uri": "urn:ietf:wg:oauth:2.0:oob",
  "refresh_token": "yyy",
  "start_date" : "2018-02-22T02:06:58.147Z"
}
```

   The `start_date` specifies the date at which the tap will begin pulling data
   (for those resources that support this).

   The `client_id`, `client_secret`, `redirect_uri`, and `refresh_token` can be generated following these [Asana instructions](https://asana.com/developers/documentation/getting-started/auth#oauth).

4. Run the Tap in Discovery Mode

    tap-asana -c config.json -d

   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode

    tap-asana -c config.json --catalog catalog-file.json

## Development

First, clone this repo. Then, in the directory:

```
$ mkvirtualenv -p python3 tap-asana
$ make dev
```

---

Copyright &copy; 2019 Stitch
