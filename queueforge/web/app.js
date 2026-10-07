const $ = (s) => document.querySelector(s);
const names = {
  pending: "待处理",
  running: "执行中",
  succeeded: "已完成",
  dead: "死信",
  cancelled: "已取消",
};
function n(tag, text) {
  const el = document.createElement(tag);
  if (text != null) el.textContent = text;
  return el;
}
async function api(path, data) {
  const r = await fetch(
      path,
      data
        ? {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(data),
          }
        : undefined,
    ),
    v = await r.json();
  if (!r.ok) throw Error(v.error);
  return v;
}
async function refresh() {
  const value = await api("/api/jobs");
  $("#metrics").replaceChildren(
    ...["pending", "running", "succeeded", "dead"].map((s) => {
      const div = n("div");
      div.className = "metric";
      div.append(n("strong", value.metrics[s] || 0), n("span", names[s]));
      return div;
    }),
  );
  $("#jobs").replaceChildren(
    ...value.jobs.map((job) => {
      const row = n("tr"),
        info = n("td"),
        actions = n("td");
      info.append(n("strong", job.task), n("div", job.idempotency));
      row.append(
        info,
        n("td", names[job.status]),
        n("td", `${job.attempts} / ${job.max_attempts}`),
        n("td", job.result || job.error || "—"),
      );
      const history = n("button", "事件");
      history.onclick = () =>
        act(async () => {
          $("#events").textContent = JSON.stringify(
            await api("/api/events/" + job.id),
            null,
            2,
          );
        });
      actions.append(history);
      for (const [action, label] of job.status === "dead"
        ? [["retry", "重新尝试"]]
        : ["pending", "running"].includes(job.status)
          ? [["cancel", "取消"]]
          : []) {
        const button = n("button", label);
        button.onclick = () =>
          act(async () => {
            await api("/api/" + action, { id: job.id });
            await refresh();
          });
        actions.append(button);
      }
      row.append(actions);
      return row;
    }),
  );
}
async function act(work) {
  try {
    await work();
  } catch (e) {
    $("#status").textContent = e.message;
  }
}
$("#form").onsubmit = (event) => {
  event.preventDefault();
  act(async () => {
    const r = await api("/api/jobs", {
      key: $("#key").value,
      task: $("#task").value,
      text: $("#text").value,
    });
    $("#status").textContent = r.created
      ? "已持久提交。可执行任务或启动独立worker。"
      : "幂等键已存在，返回原任务；没有重复创建。";
    await refresh();
  });
};
$("#work").onclick = () =>
  act(async () => {
    const r = await api("/api/work", {});
    $("#status").textContent = r.worked
      ? "任务已执行，状态已保存。"
      : "没有到期的待处理任务。";
    await refresh();
  });
$("#refresh").onclick = () => act(refresh);
act(refresh);
