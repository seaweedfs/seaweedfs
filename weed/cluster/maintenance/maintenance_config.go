package maintenance

const DefaultMasterMaintenanceScripts = `
  lock
  ec.encode -fullPercent=95 -quietFor=1h
  ec.rebuild -apply
  ec.balance -apply
  fs.log.purge -daysAgo=7
  volume.deleteEmpty -quietFor=24h -apply
  volume.balance -apply
  volume.fix.replication -apply
  s3.clean.uploads -timeAgo=24h
  unlock
`
