<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
-->
[![LinkedIn][linkedin-shield]][linkedin-url]
[![AWS CodeBuild][badge-shield]][badge-url]


<!-- PROJECT LOGO -->
<br />
<p align="center">
  <a href="https://www.linkedin.com/company/kungfuai/">
    <img src="https://media-exp1.licdn.com/dms/image/C4E0BAQEgWgybqu6dDg/company-logo_200_200/0?e=1611187200&v=beta&t=svIQxQQYJJWDvApMPTxnS3w5v_XXMHQFAvtSxzWpy6E" alt="Logo" width="80" height="80">
  </a>

  <h3 align="center">Data Compliance Service</h3>

  <p align="center">
    Your awesome ML project!
    <br />
    <a href=""><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="">View Demo</a>
    ·
    <a href="">Report Bug</a>
  </p>
</p>



<!-- TABLE OF CONTENTS -->
## Table of Contents

* [About the Project](#about-the-project)
  * [Built With](#built-with)
* [Getting Started](#getting-started)
  * [Prerequisites](#prerequisites)
  * [Installation](#installation)
* [License](#license)



### Built With
This section should list any major frameworks that you built your project using. Leave any add-ons/plugins for the acknowledgements section. Here are a few examples.
* [spaCy]()
* [Python]()
* [Docker]()



<!-- GETTING STARTED -->

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for
development and testing purposes.

Docker is used to ensure consistency across development, test, training, and production
environments. The Docker image is fully self-contained, so all required frameworks and libraries
will be installed as part of the image creation process.

### Install Requirements

Before proceeding, please install the following prerequisites:

- Install [Git](https://git-scm.com)
- Install [Docker](https://www.docker.com) version 18.0 or later
- Install [pre-commit](https://pre-commit.com)
- Install [shellcheck](https://www.shellcheck.net/)
- Install [AWS CLI](https://formulae.brew.sh/formula/awscli)

Easy install for MacOS

```shell script
brew install git
brew install docker
brew install pre-commit
brew install shellcheck
brew install awscli
```

---


### Repo Setup

- Run aws configure and set up your local aws keys.
    ```shell script
    aws configure
    # The CLI will tell you what to do
    ```
    - If you use Pycharm, install [**AWS Toolkit**](https://docs.aws.amazon.com/toolkit-for-jetbrains/latest/userguide/key-tasks.html#key-tasks-install),
    and follow their easy steps
- Ensure you login to docker for ECR in order to pull our images:
  ```shell script
  task docker:login
  ```
- Run `task build` to build the project's Docker image.
    ```shell script
    task build
    ```

#### Generating Current database schema

1. Run your docker-compose setup.
```shell
task test
```
2. Pull down our `raw-data` repo, and run the following commands:
```shell
npm install
```
3. Run migrations on your database:
```shell
npm run db:migrate:pdf
npm run db:migrate:itact
npm run db:migrate:main
```
4. Your databases should be setup and migrated
5. Run this command to generate the latest model schema as SQLAlchemy Models.
```shell
sqlacodegen --outfile src/core/db/models/itact_models.py postgresql://postgres:password@localhost:56000/postgres
sqlacodegen --outfile src/core/db/models/pdf_models.py postgresql://postgres:password@localhost:56001/postgres
sqlacodegen --outfile src/core/db/models/main_models.py postgresql://postgres:password@localhost:56002/postgres
```

#### Saving to the database

1. Use the one of the Session context-managers in the `db` module
2. e.g.
```python
with MainDbSession() as main_db:
    # ... Some mapping code to push data into a Fincen8300Rev4 SQL Alchemy Model
    models = [
      Fincen8300Rev4(...),
      Fincen8300Rev4(...),
      Fincen8300Rev4(...)
    ]
    main_db.add(models)
    main_db.commit()
```
3. This demonstrates how to select a database, and save information to our database.


#### Unit Tests

Once the Docker image is built we can run the project's unit tests to verify everything is
working. The `bin/test.sh` script will start a Docker container and execute all unit tests using
the [pytest framework](https://docs.pytest.org/en/latest/).

```sh
# run all tests
$ task test
```

By default pytest captures all output sent to `stdout` and `stderr` during test execution. This
can be disabled by passing the `-s` option.

```sh
# run tests with capture disabled and verbose
$ task test -s -vv
```

You can see a complete list of test configuration options using `--help`.

#### Interactive Shell

The `task test` script starts a Docker container in interactive mode and drops you into a bash
prompt. This can be useful when using an interactive debugger to step through code.

```sh
# run docker image in interactive bash shell
$ task test
```

<!-- CONTRIBUTING -->
## Contributing

Any contributions you make are **greatly appreciated**.

1. Create your Feature Branch (`git checkout -b <your name>/<your feature name>`)
2. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
3. Push to the Branch (`git push origin feature/AmazingFeature`)
4. Open a Pull Request



<!-- LICENSE -->
## License

Distributed with No License.


<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[license-shield]: https://img.shields.io/github/license/othneildrew/Best-README-Template.svg?style=flat-square
[badge-shield]: https://codebuild.us-gov-west-1.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiTlBBTkhOSlk5dlFjUW9jc2JsbEhlUUx1Vk5kaHVoT2kxbnVoRnRhUFRVcCt1Y0N1N2xnWFZ2R3hqOHY3dzhyamg4TlpIUjlMR0g0VzNVMlkyZ2pobzVnPSIsIml2UGFyYW1ldGVyU3BlYyI6IkE5NUlDYm5naXh6TUFIbmsiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=main
[badge-url]: https://console.amazonaws-us-gov.com/codesuite/codebuild/533333767769/projects/kungfu-vigiliant-keeper/history?region=us-gov-west-1&builds-meta=%7B%22f%22%3A%7B%22text%22%3A%22%22%7D%2C%22s%22%3A%7B%7D%2C%22n%22%3A20%2C%22i%22%3A0%7D
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=flat-square&logo=linkedin&colorB=555
[linkedin-url]: https://www.linkedin.com/company/kungfuai/
[product-screenshot]: images/screenshot.png
