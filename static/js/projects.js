// ── Projects ──

async function loadProjectButtons() {
  const el = document.getElementById('nav-project-grid');
  if (!el) return;
  try {
    const res = await fetch('/api/projects');
    const projects = await res.json();
    if (!projects.length) {
      el.innerHTML = '<div style="font-family:var(--mono);font-size:10px;color:var(--muted);padding:4px 0">no projects</div>';
      return;
    }
    el.innerHTML = projects.map(p => {
      const emoji = p.emoji || '';
      const color = p.color || 'var(--surface)';
      return `<button class="nav-project-btn" style="border-color:${color}" onclick="openProject('${p.project}')">${emoji ? emoji + ' ' : ''}${p.project}</button>`;
    }).join('');
  } catch (_) {}
}

async function openProject(projectName) {
  showTab('project');
  const projCfg = await fetch('/api/settings').then(r => r.json()).then(c => (c.projects || {})[projectName] || {}).catch(() => ({}));
  const emoji = projCfg.emoji || '';
  document.getElementById('project-detail-title').textContent = `${emoji ? emoji + ' ' : ''}${projectName}`;
  const tbody = document.getElementById('project-hist-body');
  tbody.innerHTML = '<tr><td colspan="10" style="padding:20px;text-align:center;color:var(--muted)">loading…</td></tr>';

  try {
    const res = await fetch(`/api/history?project=${encodeURIComponent(projectName)}&limit=500`);
    const rows = await res.json();

    const normalized = rows.map(r => ({
      jobid: r.job_id, name: r.job_name || '', state: r.state || '',
      elapsed: r.elapsed || '', nodes: r.nodes || '', gres: r.gres || '',
      partition: r.partition || '', submitted: r.submitted || '',
      started: r.started || '', started_local: r.started_local || '',
      ended_local: r.ended_local || '', ended_at: r.ended_at || '',
      depends_on: r.depends_on || [], dependents: r.dependents || [],
      dep_details: r.dep_details || [], project: r.project || '',
      project_color: r.project_color || '', project_emoji: r.project_emoji || '',
      _cluster: r.cluster, _pinned: true,
    }));

    const byCluster = {};
    for (const j of normalized) {
      if (!byCluster[j._cluster]) byCluster[j._cluster] = [];
      byCluster[j._cluster].push(j);
    }

    const groups = [];
    for (const [cluster, clusterJobs] of Object.entries(byCluster)) {
      for (const [label, jobs] of groupJobsByDependency(clusterJobs)) {
        groups.push({ label, cluster, jobs });
      }
    }
    groups.sort((a, b) => {
      const tsA = a.jobs.reduce((best, j) => { const t = j.submitted || j.started || ''; return t > best ? t : best; }, '');
      const tsB = b.jobs.reduce((best, j) => { const t = j.submitted || j.started || ''; return t > best ? t : best; }, '');
      if (tsA !== tsB) return tsA > tsB ? -1 : 1;
      return b.jobs.length - a.jobs.length;
    });

    if (!groups.length) {
      tbody.innerHTML = '<tr><td colspan="10" style="padding:20px;text-align:center;color:var(--muted)">no runs for this project</td></tr>';
      return;
    }

    let html = '';
    groups.forEach((g, gidx) => {
      const groupJobs = g.jobs;
      const groupLabel = `${g.cluster} · ${g.label} <span class="group-count">· ${groupJobs.length} run${groupJobs.length !== 1 ? 's' : ''}</span>`;
      if (groupJobs.length > 1) {
        html += `<tr class="group-head-row"><td colspan="10" style="padding:4px 16px">${groupLabel}</td></tr>`;
      }
      const idSet = new Set(groupJobs.map(j => j.jobid));
      const byId = {};
      for (const j of groupJobs) byId[j.jobid] = j;
      const depthMemo = {};

      groupJobs.forEach(j => {
        const st = (j.state || '').toUpperCase();
        const depth = depthInGroup(j, byId, idSet, depthMemo);
        const gpuStr = parseGpus(j.nodes, j.gres) || '—';
        const safeName = (j.name || '').replace(/'/g, "\\'");
        const logBtn = `<button class="action-btn log-btn" onclick="openLog('${g.cluster}','${j.jobid}','${safeName}')">log</button>`;
        const depBadge = depBadgeHtml(j, byId);
        const indent = depth > 0 ? `<span class="dep-indent" style="padding-left:${depth * 16}px"></span>` : '';
        const depArrow = depth > 0 ? '<span class="dep-arrow">↳</span> ' : '';
        const pinKind = isCompletedState(st) ? 'pinned-completed-row' : (isFailedLikeState(st) ? 'pinned-failed-row' : '');
        const bgClass = groupJobs.length > 1 ? ` group-bg-${gidx % 4}` : '';
        const started = fmtTime(j.started_local || j.started);
        const ended = fmtTime(j.ended_local || j.ended_at);
        const hasGpu = parseGpus(j.nodes, j.gres) !== null;
        const nameCls = hasGpu ? '' : ' name-cpu';
        

        html += `<tr class="hist-compact ${pinKind}${bgClass}">
          <td><span class="badge">${g.cluster}</span></td>
          <td class="dim">${j.jobid}</td>
          <td class="bold">${indent}${depArrow}<span class="${nameCls}" title="${j.name}">${j.name || '—'}</span></td>
          <td>${stateChip(j.state)} ${depBadge}</td>
          <td>${logBtn}</td>
          <td class="dim">${started}</td>
          <td class="dim">${ended}</td>
          <td class="dim">${j.elapsed || '—'}</td>
          <td class="dim">${gpuStr}</td>
          <td class="dim">${j.partition || '—'}</td>
        </tr>`;
      });
    });
    tbody.innerHTML = html;
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="10" style="padding:20px;color:var(--red)">Failed: ${e}</td></tr>`;
  }
}
