// Copyright (c) 2019, Frappe Technologies and contributors
// For license information, please see license.txt

frappe.ui.form.on("Data Import Custom", {
	setup(frm) {
		frappe.realtime.on("data_import_refresh", ({ data_import }) => {
			frm.import_in_progress = false;
			if (data_import !== frm.doc.name) return;
			frappe.model.clear_doc("Data Import Custom", frm.doc.name);
			frappe.model.with_doc("Data Import Custom", frm.doc.name).then(() => {
				frm.refresh();
			});
		});
		frappe.realtime.on("data_import_progress", (data) => {
			frm.import_in_progress = true;
			if (data.data_import !== frm.doc.name) {
				return;
			}
			let percent = Math.floor((data.current * 100) / data.total);
			let seconds = Math.floor(data.eta);
			let minutes = Math.floor(data.eta / 60);
			let eta_message =
				// prettier-ignore
				seconds < 60
					? __('About {0} seconds remaining', [seconds])
					: minutes === 1
						? __('About {0} minute remaining', [minutes])
						: __('About {0} minutes remaining', [minutes]);

			let message;
			if (data.success) {
				let message_args = [data.current, data.total, eta_message];
				message =
					frm.doc.import_type === "Insert New Records"
						? __("Importing {0} of {1}, {2}", message_args)
						: __("Updating {0} of {1}, {2}", message_args);
			}
			if (data.skipping) {
				message = __("Skipping {0} of {1}, {2}", [data.current, data.total, eta_message]);
			}
			frm.dashboard.show_progress(__("Import Progress"), percent, message);
			frm.page.set_indicator(__("In Progress"), "orange");
			frm.trigger("update_primary_action");

			// hide progress when complete
			if (data.current === data.total) {
				setTimeout(() => {
					frm.dashboard.hide();
					frm.refresh();
				}, 2000);
			}
		});

		frm.set_query("reference_doctype", () => {
			return {
				filters: {
					name: ["in", frappe.boot.user.can_import],
				},
			};
		});

		frm.get_field("import_file").df.options = {
			restrictions: {
				allowed_file_types: [".csv", ".xls", ".xlsx"],
			},
		};

		frm.has_import_file = () => {
			return frm.doc.import_file || frm.doc.google_sheets_url;
		};
	},

	refresh(frm) {
		frm.page.hide_icon_group();
		frm.trigger("update_indicators");
		frm.trigger("import_file");
		frm.trigger("show_import_log");
		frm.trigger("show_import_warnings");
		frm.trigger("toggle_submit_after_import");

		if (frm.doc.status != "Pending") frm.trigger("show_import_status");
		
		frm.trigger("show_report_error_button");

		if (frm.doc.status === "Partial Success") {
			frm.add_custom_button(__("Export Errored Rows"), () =>
				frm.trigger("export_errored_rows")
			);
		}

		if (frm.doc.status.includes("Success")) {
			frm.add_custom_button(__("Go to {0} List", [__(frm.doc.reference_doctype)]), () =>
				frappe.set_route("List", frm.doc.reference_doctype)
			);
		}

		/////////////////stop
		
		if (frm.doc.status === "Preprocessing" || frm.doc.status === "Running") {
            frm.add_custom_button("Stop Import", function () {
                frappe.call({
                    method: "custom_import_app.custom_import_app.doctype.data_import_custom.data_import_custom.stop_import",
                    args: { data_import: frm.doc.name },
                    callback() {
                        frappe.show_alert("Stopping import...");

						setTimeout(() => frm.reload_doc(), 3000);
                    }
                });
            });
        }

		//////////
	},

	onload_post_render(frm) {
		frm.trigger("update_primary_action");
	},

	// update_primary_action(frm) {
	// 	if (frm.is_dirty()) {
	// 		frm.enable_save();
	// 		return;
	// 	}
	// 	frm.disable_save();
	// 	if (frm.doc.status !== "Success") {
	// 		if (!frm.is_new() && frm.has_import_file()) {
	// 			let label = frm.doc.status === "Pending" ? __("Start Import") : __("Retry");
	// 			frm.page.set_primary_action(label, () => frm.events.start_import(frm));
	// 		} else {
	// 			frm.page.set_primary_action(__("Save"), () => frm.save());
	// 		}
	// 	}
	// },

	update_primary_action(frm) {
		if (frm.is_dirty() || frm.doc.status === "Pending") {
			frm.enable_save();
		} else {
			frm.disable_save();
		}

		if (frm.doc.status === "Pending") {
			if (!frm.is_new() && frm.has_import_file()) {
				frm.page.set_primary_action(__("Start Import"), () => {
					frm.events.start_import(frm);
					frappe.msgprint("Start Processing Import");
					frm.trigger("show_import_status");
					frm.refresh();
				});
				frm.page.btn_primary?.removeClass("hidden"); 
			} else {
				frm.page.set_primary_action(__("Save"), () => frm.save());
				frm.page.btn_primary?.removeClass("hidden");
			}
		} else {
			frm.page.set_primary_action("");          
			frm.page.btn_primary?.addClass("hidden"); 
		}
	},



	update_indicators(frm) {
		const indicator = frappe.get_indicator(frm.doc);
		if (indicator) {
			frm.page.set_indicator(indicator[0], indicator[1]);
		} else {
			frm.page.clear_indicator();
		}
	},

	show_import_status(frm) {
		let statusMessage = ""; 
		let jobMessage = "";    

		if (frm.doc.status=="Preprocessing" || frm.doc.status=="Running") {
			frappe.call({
				method: "custom_import_app.custom_import_app.doctype.data_import_custom.data_import_custom.get_running_import_job",
				callback: function(r) {
					let jobMessage = "";
					let statusMessage = "";

					if (r.message) {
						let job_id = r.message.job_id;
						let elapsed = r.message.elapsed_seconds;

						let minutes = Math.floor(elapsed / 60);
						let seconds = elapsed % 60;

						jobMessage = `(Job ID: ${job_id}, running ${minutes}m ${seconds}s)`;
					}

					statusMessage = frm.doc.status=="Preprocessing"
						? "Pre-processing data..."
						: frm.doc.status=="Running"
							? "Import sedang berjalan…"
							: "";

					frm.dashboard.set_headline(`${statusMessage} ${jobMessage}`);
				}
			});
		}


		frappe.call({
			method: "custom_import_app.custom_import_app.doctype.data_import_custom.data_import_custom.get_import_status",
			args: { data_import_name: frm.doc.name },
			callback: function(r) {
				let successful_records = cint(r.message.success);
				let failed_records = cint(r.message.failed);
				let total_records = cint(r.message.total_records);

				let action = frm.doc.import_type === "Insert New Records" ? "imported" : "updated";
				let importMessage;

				if (failed_records === 0) {
					importMessage = successful_records === 1
						? `Successfully ${action} 1 record. out of ${total_records}.`
						: `Successfully ${action} ${successful_records} records. out of ${total_records}.`;
				} else {
					importMessage = successful_records === 1
						? `Successfully ${action} 1 record out of ${total_records}.`
						: `Successfully ${action} ${successful_records} records out of ${total_records}.`;
				}

				if (r.message.status === "Timed Out") {
					importMessage += "<br/>Import timed out, please re-try.";
				}

				let combinedMessage = `${statusMessage} ${jobMessage}`.trim();
				if (combinedMessage) combinedMessage += " | "; 
				combinedMessage += importMessage;

				frm.dashboard.set_headline(combinedMessage);
			}
		});
	},


	show_report_error_button(frm) {
		if (frm.doc.status === "Error") {
			frappe.db
				.get_list("Error Log", {
					filters: { method: frm.doc.name },
					fields: ["method", "error"],
					order_by: "creation desc",
					limit: 1,
				})
				.then((result) => {
					if (result.length > 0) {
						frm.add_custom_button("Report Error", () => {
							let fake_xhr = {
								responseText: JSON.stringify({
									exc: result[0].error,
								}),
							};
							frappe.request.report_error(fake_xhr, {});
						});
					}
				});
		}
	},

	// start_import(frm) {
	// 	frm.call({
	// 		method: "form_start_import",
	// 		args: { data_import: frm.doc.name },
	// 		btn: frm.page.btn_primary,
	// 	}).then((r) => {
	// 		if (r.message === true) {
	// 			// frm.disable_save();
	// 		}
	// 		frm.disable_save();
	// 	});
	// },

	start_import(frm) {
		frm.call({
			method: "custom_import_app.custom_import_app.doctype.data_import_custom.data_import_custom.form_start_import",
			args: { data_import: frm.doc.name },
			btn: frm.page.btn_primary,
		}).then((r) => {
			frm.disable_save();
		});

		setTimeout(() => {
			frm.reload_doc();
		}, 1000);
	},


	download_template(frm) {
		frappe.require("data_import_tools.bundle.js", () => {
			frm.data_exporter = new frappe.data_import.DataExporter(
				frm.doc.reference_doctype,
				frm.doc.import_type
			);
		});
	},

	reference_doctype(frm) {
		frm.trigger("toggle_submit_after_import");
	},

	toggle_submit_after_import(frm) {
		frm.toggle_display("submit_after_import", false);
		let doctype = frm.doc.reference_doctype;
		if (doctype) {
			frappe.model.with_doctype(doctype, () => {
				let meta = frappe.get_meta(doctype);
				frm.toggle_display("submit_after_import", meta.is_submittable);
			});
		}
	},

	google_sheets_url(frm) {
		if (!frm.is_dirty()) {
			frm.trigger("import_file");
		} else {
			frm.trigger("update_primary_action");
		}
	},

	refresh_google_sheet(frm) {
		frm.trigger("import_file");
	},

	import_file(frm) {
		// frm.toggle_display("section_import_preview", frm.has_import_file());
		if (!frm.has_import_file()) {
			// frm.get_field("import_preview").$wrapper.empty();
			return;
		} else {
			frm.trigger("update_primary_action");
		}

		// load import preview
		// frm.get_field("import_preview").$wrapper.empty();
		// $('<span class="text-muted">')
		// 	.html(__("Loading import file..."))
		// 	.appendTo(frm.get_field("import_preview").$wrapper);

		// frm.call({
		// 	method: "get_preview_from_template",
		// 	args: {
		// 		data_import: frm.doc.name,
		// 		import_file: frm.doc.import_file,
		// 		google_sheets_url: frm.doc.google_sheets_url,
		// 	},
		// 	error_handlers: {
		// 		TimestampMismatchError() {
		// 			// ignore this error
		// 		},
		// 	},
		// }).then((r) => {
		// 	let preview_data = r.message;
		// 	frm.events.show_import_preview(frm, preview_data);
		// 	frm.events.show_import_warnings(frm, preview_data);
		// });
	},

	show_import_preview(frm, preview_data) {
		let import_log = preview_data.import_log;

		if (frm.import_preview && frm.import_preview.doctype === frm.doc.reference_doctype) {
			frm.import_preview.preview_data = preview_data;
			frm.import_preview.import_log = import_log;
			frm.import_preview.refresh();
			return;
		}

		frappe.require("data_import_tools.bundle.js", () => {
			frm.import_preview = new frappe.data_import.ImportPreview({
				wrapper: frm.get_field("import_preview").$wrapper,
				doctype: frm.doc.reference_doctype,
				preview_data,
				import_log,
				frm,
				events: {
					remap_column(changed_map) {
						let template_options = JSON.parse(frm.doc.template_options || "{}");
						template_options.column_to_field_map =
							template_options.column_to_field_map || {};
						Object.assign(template_options.column_to_field_map, changed_map);
						frm.set_value("template_options", JSON.stringify(template_options));
						frm.save().then(() => frm.trigger("import_file"));
					},
				},
			});
		});
	},

	export_errored_rows(frm) {
		open_url_post(
			"/api/method/custom_import_app.custom_import_app.doctype.data_import_custom.data_import_custom.download_errored_template",
			{
				data_import_name: frm.doc.name,
			}
		);
	},

	export_import_log(frm) {
		open_url_post(
			"/api/method/custom_import_app.custom_import_app.doctype.data_import_custom.data_import_custom.download_import_log",
			{
				data_import_name: frm.doc.name,
			}
		);
	},

	show_import_warnings(frm, preview_data) {
		let columns = preview_data.columns;
		let warnings = JSON.parse(frm.doc.template_warnings || "[]");
		warnings = warnings.concat(preview_data.warnings || []);

		frm.toggle_display("import_warnings_section", warnings.length > 0);
		if (warnings.length === 0) {
			frm.get_field("import_warnings").$wrapper.html("");
			return;
		}

		// group warnings by row
		let warnings_by_row = {};
		let other_warnings = [];
		for (let warning of warnings) {
			if (warning.row) {
				warnings_by_row[warning.row] = warnings_by_row[warning.row] || [];
				warnings_by_row[warning.row].push(warning);
			} else {
				other_warnings.push(warning);
			}
		}

		let html = "";
		html += Object.keys(warnings_by_row)
			.map((row_number) => {
				let message = warnings_by_row[row_number]
					.map((w) => {
						if (w.field) {
							let label =
								w.field.label +
								(w.field.parent !== frm.doc.reference_doctype
									? ` (${w.field.parent})`
									: "");
							return `<li>${label}: ${w.message}</li>`;
						}
						return `<li>${w.message}</li>`;
					})
					.join("");
				return `
				<div class="warning" data-row="${row_number}">
					<h5 class="text-uppercase">${__("Row {0}", [row_number])}</h5>
					<div class="body"><ul>${message}</ul></div>
				</div>
			`;
			})
			.join("");

		html += other_warnings
			.map((warning) => {
				let header = "";
				if (columns && warning.col) {
					let column_number = `<span class="text-uppercase">${__("Column {0}", [
						warning.col,
					])}</span>`;
					let column_header = columns[warning.col].header_title;
					header = `${column_number} (${column_header})`;
				}
				return `
					<div class="warning" data-col="${warning.col}">
						<h5>${header}</h5>
						<div class="body">${warning.message}</div>
					</div>
				`;
			})
			.join("");
		frm.get_field("import_warnings").$wrapper.html(`
			<div class="row">
				<div class="col-sm-10 warnings">${html}</div>
			</div>
		`);
	},

	show_failed_logs(frm) {
		frm.trigger("show_import_log");
	},

	render_import_log(frm) {
		frappe.call({
			method: "custom_import_app.custom_import_app.doctype.data_import_custom.data_import_custom.get_import_logs",
			args: {
				data_import: frm.doc.name,
			},
			callback: function(r) {
				let logs = r.message;

				if (!logs || logs.length === 0) {
					frm.get_field("import_log_preview").$wrapper.html(`<div class="text-muted">${__("No logs found")}</div>`);
					return;
				}

				frm.toggle_display("import_log_section", true);

				let tableData = logs
					.filter(log => !(frm.doc.show_failed_logs && log.success))
					.map(log => {
						let row_numbers = "";
						try {
							row_numbers = JSON.parse(log.row_indexes).join(", ");
						} catch (e) {
							row_numbers = log.row_indexes || "";
						}

						let color = log.success ? "green" : "red";

						let message = "";
						let reference_link = "";

						if (log.success) {
							message = frm.doc.import_type === "Insert New Records"
								? `Successfully imported ${log.docname}`
								: `Successfully updated ${log.docname}`;
							reference_link = frappe.utils.get_form_link(frm.doc.reference_doctype, log.docname, true);
						} else {
							let messagesArray;
							try {
								messagesArray = JSON.parse(log.messages || "[]");
								if (!Array.isArray(messagesArray)) messagesArray = [messagesArray];
							} catch(e) {
								messagesArray = [log.messages || ""];
							}

							let messages = messagesArray.map(m => {
								let title = m.title ? `<strong>${m.title}</strong>` : "";
								let msg = m.message ? `<div>${m.message}</div>` : "";
								return title + msg;
							}).join("");

							let id = frappe.dom.get_unique_id();
							message = `${messages}
								<div class="collapse" id="${id}" style="margin-top: 10px;">
									<div class="well">
										<pre>${log.exception || ""}</pre>
									</div>
								</div>`;
							reference_link = ""; 
						}

						return {
							row_numbers,
							status: log.success ? "Success" : "Failure",
							color,
							message,
							reference_link,
							traceback: log.success ? "" : log.exception || ""
						};
					});

				frm.get_field("import_log_preview").$wrapper.html(`<div id="import-log-table"></div>`);

				new Tabulator("#import-log-table", {
					data: tableData,
					layout: "fitColumns",
					columns: [
						{ title: __("Row Number"), field: "row_numbers", width: 120, 
							headerFilter:"input",
							headerFilterParams:{ elementAttributes:{ class:"form-control input-xs" } }
						},
						{ 
							title: __("Status"), 
							field: "status", 
							width: 100,
							formatter: "html",
							formatterParams: {
								html: function(cell) {
									let color = cell.getData().color;
									return `<div class="indicator ${color}">${cell.getValue()}</div>`;
								}
							},
							headerFilter:"input",
							headerFilterParams:{ elementAttributes:{ class:"form-control input-xs" } }

						},
						{ title: __("Message"), field: "message", formatter: "html", 
							headerFilter:"input",
							headerFilterParams:{ elementAttributes:{ class:"form-control input-xs" } } },
						{ 
							title: __("Reference Doctype"), 
							field: "reference_link",
							formatter: "html",
							formatterParams: {
								html: function(cell) {
									return cell.getValue();
								}
							},
							headerFilter:"input",
							headerFilterParams:{ elementAttributes:{ class:"form-control input-xs" } }
						},
						{
							title: __("Traceback"),
							field: "traceback",
							formatter: function(cell) {
								if (!cell.getValue()) return "";
								return `<button class="btn btn-default btn-xs">Show Traceback</button>`;
							},
							cellClick: function(e, cell) {
								let val = cell.getValue();
								if (!val) return;
								let d = new frappe.ui.Dialog({
									title: __("Traceback"),
									fields: [
										{ fieldtype: "HTML", fieldname: "traceback_html" }
									]
								});
								d.fields_dict.traceback_html.$wrapper.html(`<pre>${val}</pre>`);
								d.show();
							},
							headerFilter:"input",
							headerFilterParams:{ elementAttributes:{ class:"form-control input-xs" } }
						}
					],
					movableColumns: true,
					resizableRows: true,
					pagination: "local",
					paginationSize: 30,
				});
			}
		});
	},

	show_import_log(frm) {
		frm.toggle_display("import_log_section", false);

		if (frm.is_new() || frm.import_in_progress) {
			return;
		}

		frappe.call({
			method: "frappe.client.get_count",
			args: {
				doctype: "Data Import Log",
				filters: {
					data_import: frm.doc.name,
				},
			},
			callback: function (r) {
				let count = r.message;
				// if (count < 5000) {
					frm.trigger("render_import_log");
				// } else {
					frm.toggle_display("import_log_section", false);
					frm.add_custom_button(__("Export Import Log"), () =>
						frm.trigger("export_import_log")
					);
				// }
			},
		});
	},

	// poll_progress(frm) {
	// 	setTimeout(() => {
	// 		frappe.call({
	// 			method:
	// 				"custom_import_app.custom_import_app.doctype.data_import_custom.data_import_custom.get_import_status",
	// 			args: { data_import_name: frm.doc.name },
	// 			callback: function (r) {
	// 				if (!r.message) return;

	// 				let st = r.message.status;
	// 				let total = r.message.total_records;
	// 				let success = r.message.success || 0;
	// 				let failed = r.message.failed || 0;

	// 				if (st === "Pending" || success + failed < total) {
	// 					frm.import_in_progress = true;
	// 					frm.page.set_indicator(__("In Progress"), "orange");
	// 					frm.trigger("poll_progress");
	// 					console.log(r.message)
	// 					// frm.trigger("show_import_status");
	// 				} else {
	// 					frm.import_in_progress = false;
	// 					frm.refresh();
	// 				}
	// 			},
	// 		});
	// 	}, 2000);
	// }
	show_job_id(frm){
		if (frm.doc.status == "Preprocessing" || frm.doc.status == "Running") {
			frappe.call({
				method: "custom_import_app.custom_import_app.doctype.data_import_custom.data_import_custom.get_running_import_job",
				callback: function(r) {
					let message = r.message
						? `Import sedang berjalan… (Job ID: ${r.message})`
						: "Tidak ada import yang berjalan.";
					frm.dashboard.set_headline(message);
				}
			});
		}

	}

});
