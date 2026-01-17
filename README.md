# Eduri

A platform for exploring and visualizing data from the [Finnish Parliament](https://www.eduskunta.fi/EN/Pages/default.aspx).

## Setup

The sanctioned development workflow requires using a [Dev container](https://code.visualstudio.com/docs/devcontainers/containers) for running the project and its dependencies. This makes it simple to run the required PostgreSQL database and to version any needed OS-level tools.

When managing the project, all required scripts can be found in `Makefile`. Running `make` also lists the relevant ones in your terminal.

After cloning this repository, run `make database` to download all the raw data, preprocess it from the raw Eduskunta API form into the shape of our database, and then finally insert it into the DB. When developing the data pipelines, you likely need to run `make nuke` to clear it before recreating it with the new scripts. For a shorthand, you can also do `make nuke database` for both.

For the web UI, you can spin it up with `make frontend`, which starts a development server running on `localhost:4321`

## How to contribute

1. Select an issue you want to work on from the [issues view](https://github.com/boyswithoutsockson/eduri/issues) and assign it to yourself
3. Create a local branch with the name [type of change]/[name of your change] e.g. feat/add-member-page
4. Commit and push your changes
5. Open a pull request using the provided template and assign it to yourself. Ask for a review.
6. Address any comments from the reviewer.

Rinse and repeat.

## How to raise an issue

If you want to raise an issue to request a feature or notify others of a bug you should:

1. Go to the [issues view](https://github.com/boyswithoutsockson/eduri/issues)  and select "New issue" button.
2. Select the issue template.
3. Fill in the sections.
4. Post the issue.
