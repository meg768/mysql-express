#!/usr/bin/env node

let MySQL = require('mysql');
var isString = require('yow/isString');

require('dotenv').config();

class Server {
	constructor() {
		var args = require('yargs');

		args.usage('Usage: $0 [options]');

		args.usage('Usage: $0 [options]');
		args.option('listener', { alias: 'l', default: 3001, describe: 'Listen to port' });
		args.option('database', { alias: 'd', default: process.env.MYSQL_DATABASE, describe: 'MySQL database' });
		args.option('port', { alias: 'p', default: process.env.MYSQL_PORT, describe: 'MySQL port' });
		args.option('user', { alias: 'u', default: process.env.MYSQL_USER, describe: 'MySQL user' });
		args.option('host', { alias: 'h', default: process.env.MYSQL_HOST, describe: 'MySQL host' });
		args.option('password', { alias: 'w', default: process.env.MYSQL_PASSWORD, describe: 'MySQL password' });

		args.help();
		args.wrap(null);

		args.check(function (argv) {
			return true;
		});

		this.argv = args.argv;
	}

	listen() {
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
			try {
				let connection = await this.getConnection();
				let options = Object.assign({}, request.body, request.query);
				let result = await this.query(connection, options);
				response.status(200).json(result);
			} catch (error) {
				response.status(404).json(error);
			}
		});

		app.post('/upsert', async (request, response) => {
			try {
				let connection = await this.getConnection();
				let params = Object.assign({}, request.body, request.query);
				let { table, row } = params;
				let result = await this.upsert(connection, table, row);
				response.status(200).json(result);
			} catch (error) {
				response.status(404).json(error);
			}
		});

		app.listen(this.argv.listener);
	}

	upsert(connection, table, row) {
		let values = [];
		let columns = [];

		let getSQL = (row) => {
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

		return this.query(connection, getSQL(row));
	}

	query(connection, options) {
		return new Promise((resolve, reject) => {
			try {
				if (isString(options)) {
					options = { sql: options };
				}

				connection.query(options, function (error, results) {
					if (error) {
						reject(error);
					} else resolve(results);
				});
			} catch (error) {
				reject(error);
			}
		});
	}

	connect() {
		let options = {};
		options.host = this.argv.host;
		options.user = this.argv.user;
		options.password = this.argv.password;
		options.database = this.argv.database;
		options.port = this.argv.port;

		// Allow multiple statements
		options.multipleStatements = true;

		this.pool = MySQL.createPool(options);
	}

	async getConnection() {
		return new Promise((resolve, reject) => {
			this.pool.getConnection((error, connection) => {
				if (error) reject(error);
				else resolve(connection);
			});
		});
	}

	async run() {
		try {
			this.connect();

			this.listen();
		} catch (error) {
			console.error(error.stack);
		}
	}
}

let app = new Server();
app.run();
