SELECT RegistryID,
       Program,
       Interest,
       ProgramId
FROM EPAFRSInterests
where RegistryID =  :RegistryID
