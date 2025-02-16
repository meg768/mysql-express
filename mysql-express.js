#!/usr/bin/env node

let MySQL = require('mysql');
let isString = require('yow/isString');
let isArray = require('yow/isArray');

require('dotenv').config();

class Server {
	constructor() {
		var args = require('yargs');

		args.usage('Usage: $0 [options]');

		args.usage('Usage: $0 [options]');
		args.option('listener', { alias: 'l', default: process.env.MYSQSL_EXPRESS_LISTENER, describe: 'Listen to port' });
		args.option('port', { alias: 'p', default: process.env.MYSQSL_EXPRESS_PORT, describe: 'MySQL port' });
		args.option('user', { alias: 'u', default: process.env.MYSQSL_EXPRESS_USER, describe: 'MySQL user' });
		args.option('host', { alias: 'h', default: process.env.MYSQSL_EXPRESS_HOST, describe: 'MySQL host' });
		args.option('password', { alias: 'w', default: process.env.MYSQSL_EXPRESS_PASSWORD, describe: 'MySQL password' });
		args.option('token', { alias: 't', default: process.env.MYSQSL_EXPRESS_TOKEN, describe: 'Secret token' });

		args.help();
		args.wrap(null);

		args.check(function (argv) {
			return true;
		});

		this.argv = args.argv;
		this.pools = {};
	}

	log() {
		console.log.apply(this, arguments);
	}

	debug() {
		console.log.apply(this, arguments);
	}

	listen() {
		let authenticate = (request) => {
			if (request.headers.authorization != `Basic ${this.argv.token}`) {
				throw new Error('Authorization failed');
			}
		};

		const express = require('express');
		const bodyParser = require('body-parser');
		const cors = require('cors');

		const app = express();

		app.use(bodyParser.urlencoded({ limit: '50mb', extended: false }));
		app.use(bodyParser.json({ limit: '50mb' }));
		app.use(cors());

		app.get('/', function (request, result) {
			result.send('Hello World');
		});

		app.get('/query', async (request, response) => {
			let { database, ...options } = Object.assign({}, request.body, request.query);
			let connection = undefined;
			let result = undefined;

			try {
				authenticate(request);

				connection = await this.getConnection(database);
				result = await this.query(connection, options);

				response.status(200).json(result);
			} catch (error) {
				response.status(404).json({ error: error.message });
			} finally {
				this.releaseConnection(connection);
			}
		});

		app.post('/upsert', async (request, response) => {
			let { database, table, row, rows } = Object.assign({}, request.body, request.query);
			let connection = undefined;
			let result = undefined;

			try {
				authenticate(request);

				connection = await this.getConnection(database);
				result = await this.upsert(connection, table, row || rows);

				response.status(200).json(result);
			} catch (error) {
				response.status(404).json({ error: error.message });
			} finally {
				this.releaseConnection(connection);
			}
		});

		app.listen(this.argv.listener);
	}

	async upsert(connection, table, rows) {
		let getSQL = (row) => {
			let values = [];
			let columns = [];
			let sql = '';

			Object.keys(row).forEach(function (column) {
				columns.push(column);
				values.push(row[column]);
			});

			sql += MySQL.format('INSERT INTO ?? (??) VALUES (?) ', [table, columns, values]);
			sql += MySQL.format('ON DUPLICATE KEY UPDATE ');

			sql += columns
				.map((column) => {
					return MySQL.format('?? = VALUES(??)', [column, column]);
				})
				.join(',');

			return sql;
		};

		if (!isArray(rows)) {
			rows = [rows];
		}

		let multipleStatements = rows.length > 1;
		let statements = [];

		for (let row of rows) {
			statements.push(getSQL(row));
		}

		// Start transaction if more than one row
		if (multipleStatements) {
			statements.unshift('START TRANSACTION');
			statements.push('COMMIT');
		}

		let sql = statements.join(';\n');

		try {
			return await this.query(connection, sql);
		} catch (error) {
			if (multipleStatements) {
				await this.query(connection, 'ROLLBACK');
			}
			throw error;
		}
	}

	async query(connection, params) {
		let promise = new Promise((resolve, reject) => {
			try {
				if (isString(params)) {
					params = { sql: params };
				}

				let {format, sql, ...options} = params;

				if (format) {
					sql = MySQL.format(sql, format);
				}

				this.debug(params.sql);

				connection.query({sql:sql, ...options}, (error, results) => {
					if (error) {
						reject(error);
					} else resolve(results);
				});
			} catch (error) {
				reject(error);
			}
		});

		return await promise;
	}

	getPool(database) {
		if (this.pools[database] == undefined) {
			let options = {};
			options.host = this.argv.host;
			options.user = this.argv.user;
			options.password = this.argv.password;
			options.database = database;
			options.port = this.argv.port;

			// Allow multiple statements
			options.multipleStatements = true;

			this.pools[database] = MySQL.createPool(options);
		}
		return this.pools[database];
	}

	async getConnection(database) {
		let pool = this.getPool(database);

		let promise = new Promise((resolve, reject) => {
			pool.getConnection((error, connection) => {
				if (error) reject(error);
				else {
					resolve(connection);
				}
			});
		});

		return await promise;
	}

	releaseConnection(connection) {
		if (connection) {
			connection.release();
		}
	}

	async run() {
		try {
			this.listen();
		} catch (error) {
			console.error(error.stack);
		}
	}
}

let app = new Server();
app.run();
